import datetime
import datetime
import pathlib
import types
from collections import namedtuple
from pathlib import Path
from unittest.mock import Mock, call

import pytest
from otlmow_davie.Enums import Environment, AuthenticationType
from otlmow_model.Classes.Onderdeel.Bevestiging import Bevestiging
from otlmow_model.Classes.Onderdeel.LEDDriver import LEDDriver
from otlmow_model.Classes.Onderdeel.VerlichtingstoestelLED import VerlichtingstoestelLED
from otlmow_model.Classes.Onderdeel.WVLichtmast import WVLichtmast

from EMInfraDomain import EntryObject, ContentObject, AtomValueObject, AggregateIdObject
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


def test_perform_davie_aanlevering_not_tracked():
    def alt_save_to_shelf(self, entries: dict):
        for k, v in entries.items():
            self.state_db[k] = v
        raise StopIteration
    TypeTemplateToAssetProcessor._save_to_shelf = alt_save_to_shelf

    _, processor = create_processor_unittest_sqlite('used_sqlite.db')
    processor.davie_client = Mock()
    Aanlevering = namedtuple('Aanlevering', ['id'])
    processor.davie_client.create_aanlevering_employee = Mock(return_value=Aanlevering(id='0001'))
    processor.state_db = {'tracked_aanleveringen': {'2': {'aanlevering_id': '0002', 'state': 'created'}}}
    reference = 'ref'
    file_path = Path('test_perform_davie_aanlevering.py')
    event_id = '1'

    with pytest.raises(StopIteration):
        processor.perform_davie_aanlevering(reference=reference, file_path=file_path, event_id=event_id)

        assert processor.davie_client.create_aanlevering_employee.call_args_list == [call(
            niveau='LOG-1', referentie='ref', verificatorId='6c2b7c0a-11a9-443a-a96b-a1bec249c629')]
        assert processor.state_db == {'tracked_aanleveringen': {
            '1': {'aanlevering_id': '0001', 'state': 'created'},
            '2': {'aanlevering_id': '0002', 'state': 'created'}}}

