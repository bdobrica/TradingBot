from pathlib import Path
from configparser import ConfigParser

__all__ = [
    'app_config'
]

class Config(object):
    def __init__(self, config = None):
        if config is None:
            config = Path(__file__).parent.absolute() / 'config.ini'
        
        if isinstance(config, str):
            config = Path(str)
        
        if isinstance(config, Path):
            if not config.is_file():
                config = Path(__file__).parent.absolute() / 'config.ini'
            if not config.is_file():
                raise ValueError('Could not load config options from {}.'.format(config.as_posix()))

            config_path = config
            del config
            config_parser = ConfigParser()
            config_parser.read(config_path)
            config = config_parser._sections
            
        if not isinstance(config, dict):
            raise NotImplementedError('Could not create config object from a {}. Expected dict.'.format(type(config)))

        for a, b in config.items():
            if isinstance(b, (list, tuple)):
                setattr(self, a, [Config(p) if isinstance(p, dict) else p for p in b])
            else:
                setattr(self, a, Config(b) if isinstance(b, dict) else b)

app_config = Config()