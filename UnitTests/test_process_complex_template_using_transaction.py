import pathlib
from pathlib import Path
from unittest.mock import Mock

from otlmow_davie.Enums import Environment, AuthenticationType

from EMInfraRestClient import EMInfraRestClient
from TypeTemplateToAssetProcessor import TypeTemplateToAssetProcessor

THIS_FOLDER = pathlib.Path(__file__).parent


def create_processor_unittest_sqlite(sqlite_name: str) -> (EMInfraRestClient, TypeTemplateToAssetProcessor):
    sqlite_path = Path(THIS_FOLDER / sqlite_name)
    try:
        Path.unlink(Path(THIS_FOLDER / f'{sqlite_name}'))
    except FileNotFoundError:
        pass
    rest_client = Mock(spec=EMInfraRestClient)
    TypeTemplateToAssetProcessor._create_rest_client_based_on_settings = Mock()
    TypeTemplateToAssetProcessor._create_davie_client_based_on_settings = Mock()

    processor = TypeTemplateToAssetProcessor(sqlite_path=sqlite_path, settings_path=Path(),
                                             auth_type=AuthenticationType.JWT, environment=Environment.tei,
                                             postenmapping_path=Path('Postenmapping beschermbuis.state_db'))
    processor.rest_client = rest_client
    return rest_client, processor


def test_perform_davie_aanlevering_not_tracked_to_created():
    # setup
    _, processor = create_processor_unittest_sqlite('used_sqlite.db')
    processor.davie_client = Mock()
    processor._save_to_sqlite_state({'transaction_context': 'context-01_1', 'event_id': '2', 'page': '0'})
    processor._save_to_sqlite_contexts(context_id='context-01_1', starting_page= '1', last_event_id='1')
    processor._save_to_sqlite_contexts_assets(context_id='context-01_1', append='00000000-0000-0000-0000-000000000000')
    processor._save_to_sqlite_contexts_assets(context_id='context-01_1', append='00000000-0000-0000-0000-000000000001')
    processor.rest_client.import_assets_from_webservice_by_uuids = Mock(side_effect=iter([[
        {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000000-b25kZXJkZWVsI1dWTGljaHRtYXN0",
            "AIMObject.bestekPostNummer": [
                "WVlichtmast_config1"
            ],
            "AIMObject.notitie": "",
            "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/in-gebruik",
            "AIMObject.assetId": {
                "DtcIdentificator.identificator": "00000000-0000-0000-0000-000000000000-b25kZXJkZWVsI1dWTGljaHRtYXN0",
                "DtcIdentificator.toegekendDoor": "AWV"
            },
            "AIMDBStatus.isActief": True,
            "AIMObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast"
        }, {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000001-b25kZXJkZWVsI1dWTGljaHRtYXN0",
            "AIMObject.bestekPostNummer": [
                "WVlichtmast_config1"
            ],
            "AIMObject.notitie": "",
            "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/in-gebruik",
            "AIMObject.assetId": {
                "DtcIdentificator.identificator": "00000000-0000-0000-0000-000000000001-b25kZXJkZWVsI1dWTGljaHRtYXN0",
                "DtcIdentificator.toegekendDoor": "AWV"
            },
            "AIMDBStatus.isActief": True,
            "AIMObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast"
        }]]))
    processor.perform_davie_aanlevering = Mock()

    # function to test
    processor.process_complex_template_using_transaction()

    # assertions
    assert processor.perform_davie_aanlevering.call_args_list[0][1]['reference'] == 'type template processor event context-01_1'
    assert processor.perform_davie_aanlevering.call_args_list[0][1]['event_id'] == '1'
    assert processor.state_db == {'transaction_context': None, 'event_id': '1', 'page': '1'}
