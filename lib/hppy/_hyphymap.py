
from __future__ import division, print_function

from itertools import chain
from math import ceil, log10
from multiprocessing import cpu_count, current_process
from os import close, getenv, remove
from os.path import abspath, exists
from re import finditer
from subprocess import PIPE, Popen
from sys import stderr
from tempfile import mkstemp
from textwrap import dedent
from warnings import warn

from fakemp import farmout, farmworker

from ._hyphyinterface import HyphyInterface, escape, tohyphy


__all__ = ['HyphyMap', 'mpi_node_count']


def _mpienvvar():
    try:
        node_count = int(getenv('MPI', '0'))
    except ValueError:
        warn('the MPI env var should specify the desired number of nodes, defaulting to 0')
        node_count = 0
    return node_count


def _runhyphympi(cmds, node_count=None):
    if node_count is None:
        node_count = _mpienvvar()
    fd, filename = mkstemp(); close(fd)
    try:
        with open(filename, 'w') as fh:
            fh.write('MESSAGE_LOGGING = 0;\n')
            fh.write(cmds)
        p = Popen(
            ['HYPHYMPI', filename],
            env={
                'NP': '%d' % node_count,
                'PATH': getenv('PATH', '/usr/local/bin:/usr/bin:/bin')
            },
            stderr=PIPE, stdout=PIPE
        )
        pout_b, perr_b = p.communicate()
        pout = pout_b.decode('utf-8').strip()
        perr = perr_b.decode('utf-8').strip()
        retcode = p.returncode
    except OSError as e:
        print(e.strerror, file=stderr)
        retcode = 1
        pout, perr = '', e.strerror
    finally:
        # make sure to clean up
        if exists(filename):
            remove(filename)

    return (retcode, pout, perr)


def mpi_node_count():
    node_count = _mpienvvar()
    if node_count > 1:
        try:
            # use MPI_NODE_COUNT - 1 so we don't count the root process
            cmds = 'fprintf( stdout, "" + (MPI_NODE_COUNT - 1) );'
            retcode, pout, perr = _runhyphympi(cmds)
            node_count = int(pout)
        except ValueError:
            node_count = 0
    else:
        node_count = 0
    return node_count


def _quicksize(value):
    return int(log10(max(1, value)))


def _jobopts(argslist):
    if isinstance(argslist, (list, tuple)):
        # both key and value must be strings or Hyphy bugs out
        return '_jobopts = {};\n' + (
            '\n'.join('_jobopts[ %d ] = { %s };' % (
                i,
                (',\n' + (' ' * (18 + _quicksize(i)) )).join('%s: %s' % (
                    # this shit is required because keys are string-sorted, so "10" comes before "2"
                    '"%s%d"' % ('0' * (_quicksize(len(args) - 1) - _quicksize(j)), j),
                    '"%s"' % repr(v) if isinstance(v, (int, float)) else escape(v)
                ) for j, v in enumerate(args)) if args is not None else ''
            ) for i, args in enumerate(argslist))
        )
    else:
        raise ValueError('Invalid argslist supplied: "%s"' % repr(argslist))


def _globalvars(varsdict):
    return '\n'.join(tohyphy(k, v) for k, v in varsdict.items())


def _thyphyexprs(numjobs):
    return '\n'.join(dedent('''\
        if ( key == "val%(jobid)d" ) {
                return _jobvals[ %(jobid)d ];
        }
    $''') % {
        'jobid': i
    } for i in range(numjobs)).lstrip().rstrip('\n$')


def _jobdispatch(batchfile, retvar, globals, argslist, quiet=True):
    numjobs = len(argslist)
    iface = HyphyInterface()
    cmds = dedent('''\
    %(globals)s
    _job = 0;
    %(jobopts)s
    _jobvals = {};
    for ( _job = 0; _job < %(numjobs)d; _job += 1 ) {
        ExecuteAFile( "%(batchfile)s", _jobopts[ _job ] );
        _jobvals[ _job ] = %(retvar)s;
    }
    function _THyPhyAskFor( key ) {
        %(thyphyexprs)s
        return "_THyPhy_NOT_HANDLED_";
    }''') % {
        'batchfile': batchfile,
        'globals': globals,
        'jobopts': _jobopts(argslist),
        'numjobs': numjobs,
        'retvar': retvar,
        'thyphyexprs': _thyphyexprs(numjobs),
    }
    iface.queuecmd(cmds)
    iface.runqueue()

    if not quiet:
        if iface.stdout != '':
            print(iface.stdout, file=stderr)
        if iface.warnings != '':
            print(iface.warnings, file=stderr)

    if iface.stderr != '':
        raise RuntimeError(iface.stderr)

    return [ iface.getvar('val%d' % i, HyphyInterface.STRING) for i in range(numjobs) ]


class HyphyMap(object):

    def __init__(self, batchfile, retvar):
        if not exists(batchfile):
            raise ValueError('need to provide a real template batchfile!')
        self._batchfile = abspath(batchfile)
        self._retvar = retvar
        nodes = mpi_node_count()
        if nodes > 0:
            self._mpi = True
            self._nodes = nodes
        else:
            self._mpi = False
            self._nodes = cpu_count()

    @property
    def nodes(self):
        return self._nodes

    def map(self, argslist, globalvars={}, quiet=True):
        numjobs = len(argslist)
        if self._mpi:
            # message passing interface
            # don't fuck with the number of backslashes.
            # Seriously. You don't know what you're doing.
            cmds = dedent('''\
            GLOBAL_FPRINTF_REDIRECT = "/dev/null";
            _job = 0;
            %(jobopts)s
            _jobvals = {};
            _nodestates = { MPI_NODE_COUNT-1, 2 };
            _received = 0;
            while ( _received < %(numjobs)d ) {
                _recvjob = 1;
                if ( _job < %(numjobs)d ) {
                    for ( _node = 0; _node < MPI_NODE_COUNT-1; _node += 1 ) {
                        if ( _nodestates[ _node ][ 0 ] == 0 ) {
                            _recvjob = 0;
                            break;
                        }
                    }
                    if ( ! _recvjob ) {
                        _mpicmds = "";
                        _mpicmds * 256;
                        _mpicmds * ( "%(globalvars)s" );
                        _mpicmds * ( "MESSAGE_LOGGING = 0;" );
                        _mpicmds * ( "_options = " + _jobopts[ _job ] + ";" );
                        _mpicmds * ( "ExecuteAFile( "{1}%(batchfile)s"{1}, _options );" );
                        _mpicmds * ( "_retstr = "{1}"{1};" );
                        _mpicmds * ( "_retstr * 128;" );
                        _mpicmds * ( "_retstr * ( "{1}_retjob = "{1} + " + _job + " + "{1};"{1} );" );
                        _mpicmds * ( "_retstr * ( "{1}_retval = "{2}"{1} + ( %(retvar)s ^ {{ "{1}"{2}"{1}, "{1}"{3}"{1} }} ) + "{1}"{2};"{1} );" );
                        _mpicmds * ( "_retstr * 0;" );
                        _mpicmds * ( "return _retstr;" );
                        _mpicmds * 0;
                        MPISend( _node + 1, _mpicmds );
                        _nodestates[ _node ][ 0 ] = 1;
                        _nodestates[ _node ][ 1 ] = Time(0);
                        _job += 1;
                    }
                }
                if ( _recvjob ) {
                    MPIReceive( -1, _null, _retstr );
                    ExecuteCommands( _retstr );
                    _jobvals[ _retjob ] = _retval;
                    _received += 1;
                }
            }
            GLOBAL_FPRINTF_REDIRECT = "";
            fprintf( stdout, "[" + _jobvals[ 0 ] );
            for ( _job = 1; _job < %(numjobs)d; _job += 1 ) {
                fprintf ( stdout, "," + _jobvals[ _job ] );
            }
            fprintf( stdout, "]" );
            ''') % {
                'batchfile': self._batchfile,
                'globalvars': _globalvars(globalvars).replace('\n', '\\n').replace('"', '\\"'),
                'numjobs': numjobs,
                'retvar': self._retvar,
                'jobopts': _jobopts(argslist),
            }

            # this facility helps us write code that doesn't break
            # by allowing us to escape quotations appropriately:
            # by depth
            digits = set()
            for m in finditer(r'"{(\d+)}', cmds):
                digits.add(int(m.group(1)))

            for nslash in digits:
                pwr = 2 ** nslash - 1
                cmds = cmds.replace('"{%d}' % nslash, ('\\' * pwr) + '"')

            retcode, pout, perr = _runhyphympi(cmds)

            # the following no longer makes sense given the child process nature
            # of how we call hyphympi

            # if not quiet:
            #     if pout != '':
            #         print(pout, file=stderr)

            if perr != '':
                raise RuntimeError(perr)

            # this is a hideous parser for the outermost
            # json-esque array that the script above outputs

            # start at 1 because the first char is a '['
            i = 1
            nb = 0
            retarr = []
            for j, c in enumerate(pout):
                # every time we encounter a '[', increment nb
                if c == '[':
                    nb += 1
                # likewise, if we encounter a closing bracket, decrement
                if c == ']':
                    nb -= 1
                # if we're at a comma and nb == 1, we're in the outer list
                # so split from the last time we split to j-1
                if c == ',' and nb == 1:
                    retarr.append(pout[i:j])
                    i = j + 1

            # don't forget the final piece
            retarr.append(pout[i:(j + nb)])

            return retarr
        else:
            # multiprocessing
            numjobs = len(argslist)
            globals = _globalvars(globalvars)
            results = farmout(
                num=numjobs,
                setup=lambda i: (
                    _jobdispatch,
                    self._batchfile,
                    self._retvar,
                    globals,
                    (argslist[i],),
                    quiet
                ),
                worker=farmworker,
                isresult=lambda r: isinstance(r, list),
                attempts=3
            )
            return list(chain(*results))
