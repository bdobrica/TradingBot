from pathlib import Path
from configparser import ConfigParser
from sys import argv

__all__ = [
    'app_config'
]

class Config(object):
    def __init__(self, config = None):
        if config is None:
            try:
                config_arg_no = argv.index('--config') + 1
                if config_arg_no < len(argv):
                    config = Path(argv[config_arg_no]).absolute()
                else:
                    raise ValueError('Missing config file path after --config script parameter.')
            except ValueError:
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