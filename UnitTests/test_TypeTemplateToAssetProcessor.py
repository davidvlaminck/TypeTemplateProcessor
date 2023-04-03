import copy
import pathlib
import shelve
from pathlib import Path
from unittest.mock import Mock, call

import pytest

from EMInfraDomain import ListUpdateDTOKenmerkEigenschapValueUpdateDTO, KenmerkEigenschapValueUpdateDTO, ResourceRefDTO, \
    EigenschapTypedValueDTO, EntryObject, ContentObject, AtomValueObject, AggregateIdObject, \
    KenmerkEigenschapValueDTOList
from EMInfraRestClient import EMInfraRestClient
from TypeTemplateToAssetProcessor import TypeTemplateToAssetProcessor
from UnitTests.TestObjects.FakeEigenschapDTO import return_fake_eigenschap
from UnitTests.TestObjects.FakeEntryObject import fake_entry_object_with_valid_key, fake_entry_object_without_to, \
    fake_entry_object_with_two_valid_keys, fake_entry_object_without_valid_key
from UnitTests.TestObjects.FakeFeedPage import fake_feedpage_empty_entries, return_fake_feedpage_without_new_entries
from UnitTests.TestObjects.FakeKenmerkEigenschapValueDTOList import fake_attribute_list, return_fake_attribute_list, \
    fake_attribute_list2, fake_full_attribute_list, fake_full_attribute_list_two_template_keys, \
    fake_full_attribute_list_without_template_key, fake_full_attribute_list_only_bestekpostnummer, \
    fake_full_attribute_list_without_valid_template_key, fake_full_attribute_list_with_one_valid_template_key_in_list

THIS_FOLDER = pathlib.Path(__file__).parent


def test_init_TypeTemplateToAssetProcessor():
    restclient_mock = Mock(spec=EMInfraRestClient)
    processor = TypeTemplateToAssetProcessor(Path('shelve'), restclient_mock,
                                             postenmapping_path=Path('Postenmapping beschermbuis.db'))
    assert processor is not None


def test_save_last_event():
    restclient_mock = Mock(spec=EMInfraRestClient)
    restclient_mock.get_current_feedpage = Mock(return_value=fake_feedpage_empty_entries)
    processor = TypeTemplateToAssetProcessor(Path('shelve'), restclient_mock,
                                             postenmapping_path=Path('Postenmapping beschermbuis.db'))
    processor._save_to_shelf = Mock()
    processor.save_last_event()
    assert processor._save_to_shelf.call_args_list == [call(event_id='1011', page='10')]


def test_get_entries_to_process():
    list_of_entries = TypeTemplateToAssetProcessor.get_entries_to_process(current_page=fake_feedpage_empty_entries,
                                                                          event_id='1009')
    assert list_of_entries[0].id == '1010'
    assert list_of_entries[1].id == '1011'


def test_get_valid_template_key_from_feedentry_valid_key():
    restclient_mock = Mock(spec=EMInfraRestClient)
    processor = TypeTemplateToAssetProcessor(Path('shelve'), restclient_mock,
                                             postenmapping_path=Path('Postenmapping beschermbuis.db'))
    processor.postenmapping_dict = {'valid_template_key': None}

    template_key = processor.get_valid_template_key_from_feedentry(entry=fake_entry_object_with_valid_key)
    assert template_key == 'valid_template_key'


def test_get_valid_template_key_from_feedentry_invalid_key():
    restclient_mock = Mock(spec=EMInfraRestClient)
    processor = TypeTemplateToAssetProcessor(Path('shelve'), restclient_mock,
                                             postenmapping_path=Path('Postenmapping beschermbuis.db'))

    template_key = processor.get_valid_template_key_from_feedentry(entry=fake_entry_object_without_valid_key)
    assert template_key is None


def test_get_valid_template_key_from_feedentry_two_keys():
    restclient_mock = Mock(spec=EMInfraRestClient)
    processor = TypeTemplateToAssetProcessor(Path('shelve'), restclient_mock,
                                             postenmapping_path=Path('Postenmapping beschermbuis.db'))

    template_key = processor.get_valid_template_key_from_feedentry(entry=fake_entry_object_with_two_valid_keys)
    assert template_key is None


def test_get_valid_template_key_from_feedentry_without_to():
    restclient_mock = Mock(spec=EMInfraRestClient)
    processor = TypeTemplateToAssetProcessor(Path('shelve'), restclient_mock,
                                             postenmapping_path=Path('Postenmapping beschermbuis.db'))

    template_key = processor.get_valid_template_key_from_feedentry(entry=fake_entry_object_without_to)
    assert template_key is None


def test_get_current_attribute_values():
    restclient_mock = Mock(spec=EMInfraRestClient)
    restclient_mock.get_eigenschapwaarden = Mock(side_effect=return_fake_attribute_list)
    processor = TypeTemplateToAssetProcessor(Path('shelve'), restclient_mock,
                                             postenmapping_path=Path('Postenmapping beschermbuis.db'))
    processor.postenmapping_dict = {
        'valid_template_key': {'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Beschermbuis': {}},
        'valid_template_key_2': {'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#Beschermbuis': {}}}

    ns, attribute_list = processor.get_current_attribute_values(asset_uuid='00000000-0000-0000-0000-000000000001',
                                                                template_key='valid_template_key')
    assert ns == 'onderdeel'
    assert attribute_list == fake_attribute_list

    ns2, attribute_list2 = processor.get_current_attribute_values(asset_uuid='00000000-0000-0000-0000-000000000002',
                                                                  template_key='valid_template_key_2')
    assert ns2 == 'installatie'
    assert attribute_list2 == fake_attribute_list2


def test_create_update_dto_happy_flow():
    restclient_mock = Mock(spec=EMInfraRestClient)
    restclient_mock.get_eigenschap_by_uri = Mock(side_effect=return_fake_eigenschap)
    processor = TypeTemplateToAssetProcessor(Path('shelve'), restclient_mock,
                                             postenmapping_path=Path('Postenmapping beschermbuis.db'))
    processor.postenmapping_dict = {
        'valid_template_key': {
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Beschermbuis": {
                "attributen": {
                    "typeURI": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMObject.typeURI",
                        "dotnotation": "typeURI",
                        "type": "None",
                        "value": "Beschermbuis",
                        "range": None
                    },
                    "theoretischeLevensduur": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMObject.theoretischeLevensduur",
                        "dotnotation": "theoretischeLevensduur",
                        "type": "None",
                        "value": "30",
                        "range": None
                    },
                    "materiaal": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Beschermbuis.materiaal",
                        "dotnotation": "materiaal",
                        "type": "None",
                        "value": "hdpe",
                        "range": None
                    },
                    "buitendiameter": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/abstracten#Leiding.buitendiameter",
                        "dotnotation": "buitendiameter",
                        "type": "None",
                        "value": "50",
                        "range": None
                    }
                }}}}
    expected_dto = ListUpdateDTOKenmerkEigenschapValueUpdateDTO()
    expected_dto.data = [
        KenmerkEigenschapValueUpdateDTO(
            eigenschap=ResourceRefDTO(uuid='eig_bestekPostNummer'),
            kenmerkType=ResourceRefDTO(uuid='kenmerktype_uuid'),
            typedValue=EigenschapTypedValueDTO(_type='text', value=None)
        ), KenmerkEigenschapValueUpdateDTO(
            eigenschap=ResourceRefDTO(uuid='eig_materiaal'),
            kenmerkType=ResourceRefDTO(uuid='kenmerktype_uuid'),
            typedValue=EigenschapTypedValueDTO(_type='text', value='hdpe')
        ), KenmerkEigenschapValueUpdateDTO(
            eigenschap=ResourceRefDTO(uuid='eig_buitendiameter'),
            kenmerkType=ResourceRefDTO(uuid='kenmerktype_uuid'),
            typedValue=EigenschapTypedValueDTO(_type='number', value=50)
        )
    ]
    update_dto = processor.create_update_dto(template_key='valid_template_key',
                                             attribute_values=copy.deepcopy(fake_full_attribute_list))
    assert update_dto == expected_dto


def test_create_update_dto_two_valid_template_keys():
    restclient_mock = Mock(spec=EMInfraRestClient)
    restclient_mock.get_eigenschap_by_uri = Mock(side_effect=return_fake_eigenschap)
    processor = TypeTemplateToAssetProcessor(Path('shelve'), restclient_mock,
                                             postenmapping_path=Path('Postenmapping beschermbuis.db'))
    processor.postenmapping_dict = {
        'valid_template_key_2': {},
        'valid_template_key': {
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Beschermbuis": {
                "attributen": {}}}}

    update_dto = processor.create_update_dto(template_key='valid_template_key',
                                             attribute_values=copy.deepcopy(fake_full_attribute_list_two_template_keys))
    assert update_dto is None


def test_create_update_dto_without_template_keys():
    restclient_mock = Mock(spec=EMInfraRestClient)
    restclient_mock.get_eigenschap_by_uri = Mock(side_effect=return_fake_eigenschap)
    processor = TypeTemplateToAssetProcessor(Path('shelve'), restclient_mock,
                                             postenmapping_path=Path('Postenmapping beschermbuis.db'))
    processor.postenmapping_dict = {
        'valid_template_key_2': {},
        'valid_template_key': {
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Beschermbuis": {
                "attributen": {}}}}

    update_dto = processor.create_update_dto(template_key='valid_template_key',
                                             attribute_values=fake_full_attribute_list_without_template_key)
    assert update_dto is None


def test_create_update_dto_one_valid_template_key_in_list():
    restclient_mock = Mock(spec=EMInfraRestClient)
    restclient_mock.get_eigenschap_by_uri = Mock(side_effect=return_fake_eigenschap)
    processor = TypeTemplateToAssetProcessor(Path('shelve'), restclient_mock,
                                             postenmapping_path=Path('Postenmapping beschermbuis.db'))
    processor.postenmapping_dict = {
        'valid_template_key': {
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Beschermbuis": {
                "attributen": {
                    "materiaal": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Beschermbuis.materiaal",
                        "dotnotation": "materiaal",
                        "type": "None",
                        "value": "hdpe",
                        "range": None
                    }}}}}

    expected_dto = ListUpdateDTOKenmerkEigenschapValueUpdateDTO()
    expected_dto.data = [
        KenmerkEigenschapValueUpdateDTO(
            eigenschap=ResourceRefDTO(uuid='eig_bestekPostNummer'),
            kenmerkType=ResourceRefDTO(uuid='kenmerktype_uuid'),
            typedValue=EigenschapTypedValueDTO(_type='list', value=[
                {'_type': 'text', 'value': 'valid_template_key_2'}])
        ), KenmerkEigenschapValueUpdateDTO(
            eigenschap=ResourceRefDTO(uuid='eig_materiaal'),
            kenmerkType=ResourceRefDTO(uuid='kenmerktype_uuid'),
            typedValue=EigenschapTypedValueDTO(_type='text', value='hdpe')
        )
    ]
    update_dto = processor.create_update_dto(
        template_key='valid_template_key',
        attribute_values=copy.deepcopy(fake_full_attribute_list_with_one_valid_template_key_in_list))
    assert update_dto == expected_dto


def test_create_update_dto_without_valid_template_keys():
    restclient_mock = Mock(spec=EMInfraRestClient)
    restclient_mock.get_eigenschap_by_uri = Mock(side_effect=return_fake_eigenschap)
    processor = TypeTemplateToAssetProcessor(Path('shelve'), restclient_mock,
                                             postenmapping_path=Path('Postenmapping beschermbuis.db'))
    processor.postenmapping_dict = {
        'valid_template_key_2': {},
        'valid_template_key': {
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Beschermbuis": {
                "attributen": {}}}}

    update_dto = processor.create_update_dto(template_key='valid_template_key',
                                             attribute_values=fake_full_attribute_list_without_valid_template_key)
    assert update_dto is None


def test_create_update_dto_not_implemented_datatype():
    restclient_mock = Mock(spec=EMInfraRestClient)
    restclient_mock.get_eigenschap_by_uri = Mock(side_effect=return_fake_eigenschap)
    processor = TypeTemplateToAssetProcessor(Path('shelve'), restclient_mock,
                                             postenmapping_path=Path('Postenmapping beschermbuis.db'))
    processor.postenmapping_dict = {
        'valid_template_key': {
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Beschermbuis": {
                "attributen": {
                    "theoretischeLevensduur": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMObject.not_implemented_type",
                        "dotnotation": "not_implemented_type",
                        "type": "None",
                        "value": "30",
                        "range": None
                    }}}}}

    with pytest.raises(NotImplementedError) as exc_info:
        processor.create_update_dto(template_key='valid_template_key',
                                    attribute_values=copy.deepcopy(fake_full_attribute_list))
    assert 'no implementation yet for' in str(exc_info.value)


def test_create_update_dto_complex_datatype():
    restclient_mock = Mock(spec=EMInfraRestClient)
    restclient_mock.get_eigenschap_by_uri = Mock(side_effect=return_fake_eigenschap)
    processor = TypeTemplateToAssetProcessor(Path('shelve'), restclient_mock,
                                             postenmapping_path=Path('Postenmapping beschermbuis.db'))
    processor.postenmapping_dict = {
        'valid_template_key': {
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Beschermbuis": {
                "attributen": {
                    "theoretischeLevensduur": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMObject.not_implemented_type",
                        "dotnotation": ".",
                        "type": "None",
                        "value": "30",
                        "range": None
                    }}}}}
    with pytest.raises(NotImplementedError) as exc_info:
        processor.create_update_dto(template_key='valid_template_key',
                                    attribute_values=copy.deepcopy(fake_full_attribute_list))
    assert 'complex datatypes not yet implemented' in str(exc_info.value)


def test_create_update_dto_boolean_datatype():
    restclient_mock = Mock(spec=EMInfraRestClient)
    restclient_mock.get_eigenschap_by_uri = Mock(side_effect=return_fake_eigenschap)
    processor = TypeTemplateToAssetProcessor(Path('shelve'), restclient_mock,
                                             postenmapping_path=Path('Postenmapping beschermbuis.db'))
    processor.postenmapping_dict = {
        'valid_template_key': {
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Beschermbuis": {
                "attributen": {
                    "boolean_type": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMObject.boolean_type",
                        "dotnotation": "boolean_type",
                        "type": "None",
                        "value": "True",
                        "range": None
                    }}}}}

    expected_dto = ListUpdateDTOKenmerkEigenschapValueUpdateDTO()
    expected_dto.data = [
        KenmerkEigenschapValueUpdateDTO(
            eigenschap=ResourceRefDTO(uuid='eig_bestekPostNummer'),
            kenmerkType=ResourceRefDTO(uuid='kenmerktype_uuid'),
            typedValue=EigenschapTypedValueDTO(_type='text', value=None)
        ), KenmerkEigenschapValueUpdateDTO(
            eigenschap=ResourceRefDTO(uuid='eig_boolean_type'),
            kenmerkType=ResourceRefDTO(uuid='kenmerktype_uuid'),
            typedValue=EigenschapTypedValueDTO(_type='boolean', value=True)
        )
    ]

    update_dto = processor.create_update_dto(template_key='valid_template_key',
                                             attribute_values=fake_full_attribute_list_only_bestekpostnummer)
    assert update_dto == expected_dto


def test_save_to_shelf():
    _, processor, shelve_path = create_processor_unittest_shelve(shelve_name='db_unittests_5')
    processor._save_to_shelf(page='123')
    processor._save_to_shelf(event_id='123')
    with shelve.open(str(shelve_path)) as db:
        assert db['page'] == '123'
        assert db['event_id'] == '123'


def test_save_last_event_called_with_process():
    _, processor, _ = create_processor_unittest_shelve(shelve_name='db_unittests_4')

    def exit_loop():
        raise StopIteration

    processor.save_last_event = Mock()
    processor.save_last_event.side_effect = exit_loop

    processor.process()
    assert processor.save_last_event.called


def test_process_loop_no_events():
    rest_client, processor, _ = create_processor_unittest_shelve(shelve_name='db_unittests_3')
    processor._save_to_shelf(page='10', event_id='1010')

    def exit_loop():
        raise StopIteration

    processor.wait_seconds = Mock()
    processor.wait_seconds.side_effect = exit_loop
    rest_client.get_feedpage = Mock()
    rest_client.get_feedpage.side_effect = return_fake_feedpage_without_new_entries

    processor.process()
    assert processor.wait_seconds.called


def create_processor_unittest_shelve(shelve_name: str):
    shelve_path = Path(THIS_FOLDER / shelve_name)
    try:
        Path.unlink(Path(THIS_FOLDER / f'{shelve_name}.db'))
    except FileNotFoundError:
        pass
    rest_client = Mock(spec=EMInfraRestClient)
    processor = TypeTemplateToAssetProcessor(shelve_path=shelve_path, rest_client=rest_client,
                                             postenmapping_path=Path('Postenmapping beschermbuis.db'))
    return rest_client, processor, shelve_path


def test_process_loop_no_events_on_next_page():
    rest_client, processor, shelve_path = create_processor_unittest_shelve(shelve_name='db_unittests_1')
    processor._save_to_shelf(page='20', event_id='1010')

    def exit_loop():
        raise StopIteration

    processor.wait_seconds = Mock()
    processor.wait_seconds.side_effect = exit_loop
    rest_client.get_feedpage = Mock()
    rest_client.get_feedpage.side_effect = return_fake_feedpage_without_new_entries

    processor.process()
    assert processor.wait_seconds.called
    with shelve.open(str(shelve_path)) as db:
        assert db['page'] == '21'


def test_sleep():
    _, processor, _ = create_processor_unittest_shelve(shelve_name='db_unittests_2')
    processor.wait_seconds(0)


def test_process_all_entries_type_to_ignore():
    _, processor, _ = create_processor_unittest_shelve(shelve_name='db_unittests_6')
    local_db = {}
    processor.process_all_entries(db=local_db, entries_to_process=[
        EntryObject(id='id', content=ContentObject(value=AtomValueObject(_type='IGNORED_TYPE', _typeVersion=1)))])
    assert local_db['event_id'] == 'id'


def test_process_all_entries_invalid_template_key():
    _, processor, _ = create_processor_unittest_shelve(shelve_name='db_unittests_7')
    local_db = {}
    processor.get_valid_template_key_from_feedentry = Mock()
    processor.get_valid_template_key_from_feedentry.side_effect = lambda _: None

    processor.process_all_entries(db=local_db, entries_to_process=[
        EntryObject(id='id', content=ContentObject(value=AtomValueObject(
            _type='ASSET_KENMERK_EIGENSCHAP_VALUES_UPDATED', _typeVersion=1)))])
    assert local_db['event_id'] == 'id'


def test_process_all_entries_valid_template_key():
    _, processor, _ = create_processor_unittest_shelve(shelve_name='db_unittests_8')
    local_db = {}
    processor.get_current_attribute_values = Mock()
    processor.get_current_attribute_values.side_effect = \
        lambda asset_uuid, template_key: ('onderdeel', KenmerkEigenschapValueDTOList())
    processor.get_valid_template_key_from_feedentry = Mock()
    processor.get_valid_template_key_from_feedentry.side_effect = lambda _: 'valid_template_key'
    processor.create_update_dto = Mock()
    processor.create_update_dto.side_effect = \
        lambda template_key, attribute_values: ListUpdateDTOKenmerkEigenschapValueUpdateDTO()

    processor.process_all_entries(db=local_db, entries_to_process=[
        EntryObject(id='id', content=ContentObject(value=AtomValueObject(
            _type='ASSET_KENMERK_EIGENSCHAP_VALUES_UPDATED', _typeVersion=1,
            aggregateId=AggregateIdObject(uuid="asset-uuid-0000"))))])
    assert local_db['event_id'] == 'id'
