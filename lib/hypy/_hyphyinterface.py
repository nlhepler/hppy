
from copy import deepcopy
from os import getcwd
from os.path import abspath, exists, join

import HyPhy as hp


__all__ = ['HyphyInterface', 'escape']


def escape(value):
    if not isinstance(value, (float, int, str)):
        raise ValueError("Cannot escape types other than float, int, or str: '%s'" % repr(value))
    return '"%s"' % value.replace('"', r'\"') if isinstance(value, str) else repr(value)


class HyphyInterface(object):

    MATRIX = hp.THYPHY_TYPE_MATRIX
    NUMBER = hp.THYPHY_TYPE_NUMBER
    STRING = hp.THYPHY_TYPE_STRING

    def __init__(self, batchfile=None, num_cpus=1):
        self._batchfile = batchfile
        self._execstr = ''
        self._instance = hp._THyPhy(join(abspath(hp.__file__), 'res'), num_cpus)
        self._stdout = ''
        self._stderr = ''
        self._warnings = ''

    def _fetchenv(self):
        self._stdout += self._instance.GetStdout().sData.strip()
        self._stderr += self._instance.GetErrors().sData.strip()
        self._warnings += self._instance.GetWarnings().sData.strip()

    def getvar(self, variable, typ):
        _res = self._instance.AskFor(variable)
        if typ not in (HyphyInterface.MATRIX, HyphyInterface.NUMBER, HyphyInterface.STRING):
            raise ValueError("Unknown type supplied: please use one of HyphyInterface.{MATRIX,NUMBER,STRING}")
        if (self._instance.CanCast(_res, typ)):
            res = self._instance.CastResult(_res, typ)
            if typ == HyphyInterface.STRING:
                return res.castToString().sData
            elif typ == HyphyInterface.NUMBER:
                return res.castToNumber().nValue
            elif typ == HyphyInterface.MATRIX:
                hymat = res.castToMatrix()
                mat = None
                one_d = hymat.mRows == 1, hymat.mCols == 1
                if any(one_d):
                    dim = max(hymat.mRows, hymat.mCols)
                    mat = [0.] * dim
                    if one_d[0]:
                        for i in range(dim):
                            mat[i] = hymat.MatrixCell(0, i)
                    else:
                        for i in range(dim):
                            mat[i] = hymat.MatrixCell(i, 0)
                else:
                    mat = [[None] * hymat.mCols] * hymat.mRows
                    for i in range(hymat.mRows):
                        for j in range(hymat.mCols):
                            mat[i][j] = hymat.MatrixCell(i, j)
                return mat
            else:
                # dead code, we assume
                assert(0)
        else:
            raise RuntimeError("Cast failed in HyphyInterface, assume an incorrect type was supplied for variable `%s'" % variable)

    def queuecmd(self, execstr):
        self._execstr += execstr

    def queuestralloc(self, name, size):
        self._execstr += '%s = "";\n%s * %d;\n' % (name, name, size)

    def queuevar(self, name, value):
        execstr = ''
        if isinstance(value, (list, tuple)):
            for i, v in enumerate(value):
                if isinstance(v, (list, tuple)):
                    if i == 0:
                        execstr += "%s = {%d,%d};\n" % (name, len(value), len(v))
                    for j, w in enumerate(v):
                        if not isinstance(w, (float, int)):
                            raise ValueError("2D matrices must contain values of type float or int")
                        execstr += "%s[%d][%d] = %s;\n" % (name, i, j, repr(w))
                elif isinstance(v, (float, int, str)):
                    if i == 0:
                        execstr += "%s = {};\n" % name
                    execstr += "%s[%d] = %s;\n" % (name, i, escape(v))
                else:
                    raise ValueError("Lists must contain values of type float, int, or str")
        elif isinstance(value, dict):
            initted = False
            for k, v in value.items():
                if not initted:
                    execstr += "%s = {};\n" % name
                    initted = True
                if not isinstance(k, (int, str)):
                    raise ValueError("Dictionary keys must be values of type int or str")
                if not isinstance(v, (float, int, str)):
                    raise ValueError("Dictionaries must contain values of type float, int, or str")
                execstr += "%s[%s] = %s;\n" % (name, escape(k), escape(v))
        elif isinstance(value, (float, int, str)):
            execstr += "%s = %s;\n" % (name, escape(value))
        else:
            raise ValueError("inject() supports only floats, ints, and strs; lists of the same; and 2d matrices of floats or ints")
        self._execstr += execstr

    def reset(self):
        self._instance.ClearAll()
        self._stderr = ''
        self._stdout = ''
        self._warnings = ''

    def runqueue(self, *args, **kwargs):
        batchfile = None
        execstr = None

        errstr = "runqueue() takes a two optional arguments: a HyPhy batchfile or a str containing HyPhy commands"

        if batchfile is None and self._execstr == '':
            if 'batchfile' in kwargs:
                batchfile = kwargs['batchfile']
            elif 'execstr' in kwargs:
                execstr = kwargs['execstr']
            elif len(args) > 0 and exists(args[0]):
                batchfile = args[0]
            elif len(args) > 0 and args[0].strip()[-1] == ';':
                execstr = args[0]
            else:
                raise ValueError(errstr)

        # if we weren't given a batchfile in runqueue,
        # grab the instance one
        if batchfile is None:
            batchfile = self._batchfile

        if batchfile is not None:
            if exists(batchfile):
                with open(batchfile) as fh:
                    self._execstr += fh.read()
            else:
                raise ValueError("Invalid batchfile `%s', it doesn't exist!" % batchfile)
        elif execstr is not None:
            self._execstr += execstr

        ret = self._instance.ExecuteBF(self._execstr)
        HyphyInterface._fetchenv(self)
        return ret

    @property
    def stderr(self):
        return deepcopy(self._stderr)

    @property
    def stdout(self):
        return deepcopy(self._stdout)

    @property
    def warnings(self):
        return deepcopy(self._warnings)
