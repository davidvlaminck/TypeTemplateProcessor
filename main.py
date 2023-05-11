import logging
import pathlib
from pathlib import Path

from otlmow_davie.Enums import AuthenticationType, Environment

from TypeTemplateToAssetProcessor import TypeTemplateToAssetProcessor

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')
    settings_path = Path('/home/davidlinux/Documents/AWV/resources/settings_TypeTemplateProcessor.json')

    this_directory = pathlib.Path(__file__).parent
    processor = TypeTemplateToAssetProcessor(sqlite_path=Path(this_directory / 'local_tei.state_db'),
                                             settings_path=settings_path,
                                             auth_type=AuthenticationType.JWT,
                                             environment=Environment.tei,
                                             postenmapping_path=Path('Postenmapping beschermbuis.db'))
    processor.process()
