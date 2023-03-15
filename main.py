import logging
from pathlib import Path

from EMInfraRestClient import EMInfraRestClient
from RequestHandler import RequestHandler
from RequesterFactory import RequesterFactory
from SettingsManager import SettingsManager
from TypeTemplateToAssetProcessor import TypeTemplateToAssetProcessor

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    settings_manager = SettingsManager(
        settings_path='/home/davidlinux/Documents/AWV/resources/settings_EMInfraClient.json')
    requester = RequesterFactory.create_requester(settings=settings_manager.settings, auth_type='JWT', env='tei')
    request_handler = RequestHandler(requester)
    rest_client = EMInfraRestClient(request_handler=request_handler)

    processor = TypeTemplateToAssetProcessor(Path('shelve'), rest_client, postenmapping_path=Path('Postenmapping beschermbuis.db'))
    processor.process()

    print()
