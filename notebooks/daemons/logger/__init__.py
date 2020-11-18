from datetime import datetime
from os import mkdir, linesep
from pathlib import Path

class LoggerLevels:
    NONE = 0
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50

class Logger:
    def __init__(self, path = None, level = LoggerLevels.NONE):
        if isinstance(path, str):
            path = Path(path).absolute()
        if not isinstance(path, Path):
            raise NotImplementedError('The parameter path should be a string or a Path like object, {} provided.'.format(type(path)))
        
        dir_path = path.parent
        if not dir_path.is_dir():
            mkdir(str(dir_path))

        self.path = path
        self.level = level
    
    def _prefix(self):
        return str(datetime.now())
    
    def _message(self, level, message):
        out = '{prefix} - {level} - {message}'.format(
            prefix = self._prefix(),
            level = level,
            message = message
        )

        if self.path is None:
            print(out)
        else:
            with self.path.open('a') as fp:
                fp.write(out + linesep)
    
    def debug(self, message):
        if self.level > LoggerLevels.DEBUG:
            return
        self._message('DEBUG', message)
    
    def info(self, message):
        if self.level > LoggerLevels.INFO:
            return
        self._message('INFO', message)
    
    def warning(self, message):
        if self.level > LoggerLevels.WARNING:
            return
        self._message('WARNING', message)
    
    def error(self, message):
        if self.level > LoggerLevels.ERROR:
            return
        self._message('ERROR', message)
    
    def critical(self, message):
        if self.level > LoggerLevels.CRITICAL:
            return
        self._message('CRITICAL', message)