import logging
import pathlib
from pathlib import Path

from otlmow_davie.Enums import AuthenticationType, Environment

from TypeTemplateToAssetProcessor import TypeTemplateToAssetProcessor

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    settings_path = Path('/home/davidlinux/Documents/AWV/resources/settings_TypeTemplateProcessor.json')

    this_directory = pathlib.Path(__file__).parent
    processor = TypeTemplateToAssetProcessor(shelve_path=Path(this_directory / 'shelve'),
                                             settings_path=settings_path,
                                             auth_type=AuthenticationType.JWT,
                                             environment=Environment.tei,
                                             postenmapping_path=Path('Postenmapping beschermbuis.db'))
    processor.process()
