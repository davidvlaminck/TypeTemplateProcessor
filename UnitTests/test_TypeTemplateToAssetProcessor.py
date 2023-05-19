import copy
import datetime
import pathlib
import sqlite3
from pathlib import Path
from unittest.mock import Mock, call

import pytest
from otlmow_davie.Enums import Environment, AuthenticationType

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


def test_init_TypeTemplateToAssetProcessor():
    _, processor = create_processor_unittest_sqlite('unused_sqlite.db')
    assert processor is not None
    assert Path(THIS_FOLDER / 'unused_sqlite.db').exists()


def test_save_last_event_called_with_correct_args():
    restclient_mock, processor = create_processor_unittest_sqlite('unused_sqlite.db')
    restclient_mock.get_current_feedpage = Mock(return_value=fake_feedpage_empty_entries)
    processor._save_to_sqlite_state = Mock()
    processor.save_last_event()
    assert processor._save_to_sqlite_state.call_args_list == [call(entries={'event_id': '1011', 'page': '10'})]


def test_get_entries_to_process():
    list_of_entries = TypeTemplateToAssetProcessor.get_entries_to_process(current_page=fake_feedpage_empty_entries,
                                                                          event_id='1009')
    assert list_of_entries[0].id == '1010'
    assert list_of_entries[1].id == '1011'


def test_get_valid_template_key_from_feedentry_valid_key():
    _, processor = create_processor_unittest_sqlite('unused_sqlite.db')
    processor.postenmapping_dict = {'valid_template_key': None}

    template_key = processor.get_valid_template_key_from_feedentry(entry=fake_entry_object_with_valid_key)
    assert template_key == 'valid_template_key'


def test_get_valid_template_key_from_feedentry_invalid_key():
    _, processor = create_processor_unittest_sqlite('unused_sqlite.db')
    template_key = processor.get_valid_template_key_from_feedentry(entry=fake_entry_object_without_valid_key)
    assert template_key is None


def test_get_valid_template_key_from_feedentry_two_keys():
    _, processor = create_processor_unittest_sqlite('unused_sqlite.db')
    template_key = processor.get_valid_template_key_from_feedentry(entry=fake_entry_object_with_two_valid_keys)
    assert template_key is None


def test_get_valid_template_key_from_feedentry_without_to():
    _, processor = create_processor_unittest_sqlite('unused_sqlite.db')
    template_key = processor.get_valid_template_key_from_feedentry(entry=fake_entry_object_without_to)
    assert template_key is None


def test_get_current_attribute_values():
    restclient_mock, processor = create_processor_unittest_sqlite('unused_sqlite.db')
    restclient_mock.get_eigenschapwaarden = Mock(side_effect=return_fake_attribute_list)
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
    restclient_mock, processor = create_processor_unittest_sqlite('unused_sqlite.db')
    restclient_mock.get_eigenschap_by_uri = Mock(side_effect=return_fake_eigenschap)
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
    restclient_mock, processor = create_processor_unittest_sqlite('unused_sqlite.db')
    restclient_mock.get_eigenschap_by_uri = Mock(side_effect=return_fake_eigenschap)
    processor.postenmapping_dict = {
        'valid_template_key_2': {},
        'valid_template_key': {
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Beschermbuis": {
                "attributen": {}}}}

    update_dto = processor.create_update_dto(template_key='valid_template_key',
                                             attribute_values=copy.deepcopy(fake_full_attribute_list_two_template_keys))
    assert update_dto is None


def test_create_update_dto_without_template_keys():
    restclient_mock, processor = create_processor_unittest_sqlite('unused_sqlite.db')
    restclient_mock.get_eigenschap_by_uri = Mock(side_effect=return_fake_eigenschap)
    processor.postenmapping_dict = {
        'valid_template_key_2': {},
        'valid_template_key': {
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Beschermbuis": {
                "attributen": {}}}}

    update_dto = processor.create_update_dto(template_key='valid_template_key',
                                             attribute_values=fake_full_attribute_list_without_template_key)
    assert update_dto is None


def test_create_update_dto_one_valid_template_key_in_list():
    restclient_mock, processor = create_processor_unittest_sqlite('unused_sqlite.db')
    restclient_mock.get_eigenschap_by_uri = Mock(side_effect=return_fake_eigenschap)
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
    restclient_mock, processor = create_processor_unittest_sqlite('unused_sqlite.db')
    restclient_mock.get_eigenschap_by_uri = Mock(side_effect=return_fake_eigenschap)
    processor.postenmapping_dict = {
        'valid_template_key_2': {},
        'valid_template_key': {
            "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Beschermbuis": {
                "attributen": {}}}}

    update_dto = processor.create_update_dto(template_key='valid_template_key',
                                             attribute_values=fake_full_attribute_list_without_valid_template_key)
    assert update_dto is None


def test_create_update_dto_not_implemented_datatype():
    restclient_mock, processor = create_processor_unittest_sqlite('unused_sqlite.db')
    restclient_mock.get_eigenschap_by_uri = Mock(side_effect=return_fake_eigenschap)
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
    restclient_mock, processor = create_processor_unittest_sqlite('unused_sqlite.db')
    restclient_mock.get_eigenschap_by_uri = Mock(side_effect=return_fake_eigenschap)
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
    restclient_mock, processor = create_processor_unittest_sqlite('unused_sqlite.db')
    restclient_mock.get_eigenschap_by_uri = Mock(side_effect=return_fake_eigenschap)
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


def test_save_to_sqlite_state():
    sqlite_path = THIS_FOLDER / 'used_sqlite.db'
    _, processor = create_processor_unittest_sqlite('used_sqlite.db')
    processor._save_to_sqlite_state({'page': '123'})
    processor._save_to_sqlite_state({'event_id': '456'})

    conn = sqlite3.connect(sqlite_path)
    c = conn.cursor()
    c.execute('''SELECT value from state where name = 'page' ''')
    page_value = c.fetchone()[0]
    c.execute('''SELECT value from state where name = 'event_id' ''')
    event_id_value = c.fetchone()[0]
    conn.commit()
    conn.close()

    assert page_value == '123'
    assert event_id_value == '456'
    assert processor.state_db == {'page': '123', 'event_id': '456'}

    processor._save_to_sqlite_state({'event_id': '789'})

    conn = sqlite3.connect(sqlite_path)
    c = conn.cursor()
    c.execute('''SELECT value from state where name = 'event_id' ''')
    event_id_value = c.fetchone()[0]
    conn.commit()
    conn.close()

    assert event_id_value == '789'
    assert processor.state_db == {'page': '123', 'event_id': '789'}


def test_save_last_event_called_with_process():
    _, processor = create_processor_unittest_sqlite('used_sqlite.db')

    def exit_loop():
        raise StopIteration

    processor.save_last_event = Mock()
    processor.save_last_event.side_effect = exit_loop

    processor.process()
    assert processor.save_last_event.called


def test_process_loop_no_events():
    rest_client, processor = create_processor_unittest_sqlite('used_sqlite.db')
    processor._save_to_sqlite_state({'page': '20', 'event_id': '1010'})

    def exit_loop():
        raise StopIteration

    processor.wait_seconds = Mock()
    processor.wait_seconds.side_effect = exit_loop
    rest_client.get_feedpage = Mock()
    rest_client.get_feedpage.side_effect = return_fake_feedpage_without_new_entries

    processor.process()
    assert processor.wait_seconds.called


def test_process_loop_no_events_on_next_page():
    rest_client, processor = create_processor_unittest_sqlite('used_sqlite.db')
    processor._save_to_sqlite_state({'page': '20', 'event_id': '1010'})

    def exit_loop():
        raise StopIteration

    processor.wait_seconds = Mock()
    processor.wait_seconds.side_effect = exit_loop
    rest_client.get_feedpage = Mock()
    rest_client.get_feedpage.side_effect = return_fake_feedpage_without_new_entries

    processor.process()
    assert processor.wait_seconds.called
    assert processor.state_db['page'] == '21'


def test_sleep():
    _, processor = create_processor_unittest_sqlite('unused_sqlite.db')
    processor.wait_seconds(0)


def test_process_all_entries_type_to_ignore():
    _, processor = create_processor_unittest_sqlite('used_sqlite.db')
    processor.process_all_entries(entries_to_process=[
        EntryObject(id='id', content=ContentObject(value=AtomValueObject(_type='IGNORED_TYPE', _typeVersion=1)))])
    assert processor.state_db['event_id'] == 'id'


def test_process_all_entries_invalid_template_key():
    _, processor = create_processor_unittest_sqlite('used_sqlite.db')
    processor.get_valid_template_key_from_feedentry = Mock()
    processor.get_valid_template_key_from_feedentry.side_effect = lambda _: None

    processor.process_all_entries(entries_to_process=[
        EntryObject(id='id', content=ContentObject(value=AtomValueObject(
            _type='ASSET_KENMERK_EIGENSCHAP_VALUES_UPDATED', _typeVersion=1)))])
    assert processor.state_db['event_id'] == 'id'


def test_process_all_entries_valid_template_key():
    _, processor = create_processor_unittest_sqlite('used_sqlite.db')
    processor.state_db = {'transaction_context': None}
    processor.postenmapping_dict = {'valid_template_key': {}}
    processor.get_current_attribute_values = Mock()
    processor.get_current_attribute_values.side_effect = \
        lambda asset_uuid, template_key: ('onderdeel', KenmerkEigenschapValueDTOList())
    processor.get_valid_template_key_from_feedentry = Mock()
    processor.get_valid_template_key_from_feedentry.side_effect = lambda _: 'valid_template_key'
    processor.create_update_dto = Mock()
    processor.create_update_dto.side_effect = \
        lambda template_key, attribute_values: ListUpdateDTOKenmerkEigenschapValueUpdateDTO()

    processor.process_all_entries(entries_to_process=[
        EntryObject(id='id', content=ContentObject(value=AtomValueObject(
            _type='ASSET_KENMERK_EIGENSCHAP_VALUES_UPDATED', _typeVersion=1,
            aggregateId=AggregateIdObject(uuid='asset-uuid-0000'))))])
    assert processor.state_db['event_id'] == 'id'


def test_process_all_entries_no_entries_with_transaction_context():
    _, processor = create_processor_unittest_sqlite('used_sqlite.db')
    processor.process_complex_template_using_transaction = Mock()
    processor.state_db = {'transaction_context': 'something'}
    processor.process_all_entries(entries_to_process=[])
    assert processor.process_complex_template_using_transaction.called


def test_process_all_entries_transaction_context_add_entry():
    # setup
    _, processor = create_processor_unittest_sqlite('used_sqlite.db')
    processor.process_complex_template_using_transaction = Mock()
    processor.process_complex_template_using_transaction.side_effect = lambda: None
    processor.get_valid_template_key_from_feedentry = Mock()
    processor.get_valid_template_key_from_feedentry.side_effect = lambda _: 'valid_template_key'

    processor._save_to_sqlite_state({'transaction_context': 'context_01_1'})
    processor._save_to_sqlite_contexts(context_id='context_01_1',
                                       starting_page='1',
                                       last_event_id='1')
    processor._save_to_sqlite_contexts_assets(context_id='context_01_1', append='asset-uuid-0001')

    # function to test
    processor.process_all_entries(entries_to_process=[
        EntryObject(id='id', content=ContentObject(value=AtomValueObject(
            _type='ASSET_KENMERK_EIGENSCHAP_VALUES_UPDATED', _typeVersion=1, contextId='context_01',
            aggregateId=AggregateIdObject(uuid='asset-uuid-0002'))))])

    # assertions
    assert processor.state_db == {'event_id': 'id', 'transaction_context': 'context_01_1'}
    conn = sqlite3.connect(THIS_FOLDER / 'used_sqlite.db')
    c = conn.cursor()
    c.execute('''SELECT id, starting_page, last_event_id FROM contexts''')
    contexts_row = c.fetchone()
    c.execute('''SELECT context_id, asset_uuid FROM contexts_assets''')
    contexts_assets_rows = [row for row in c.fetchall()]
    conn.commit()
    conn.close()
    assert contexts_row == ('context_01_1', '1', 'id')
    assert contexts_assets_rows == [('context_01_1', 'asset-uuid-0001'), ('context_01_1', 'asset-uuid-0002')]


def test_process_all_entries_transaction_context_process_entry_without_context_last_processed_ok():
    # setup
    _, processor = create_processor_unittest_sqlite('used_sqlite.db')
    processor.process_complex_template_using_transaction = Mock()
    processor.process_complex_template_using_transaction.side_effect = lambda: None
    processor.get_valid_template_key_from_feedentry = Mock()
    processor.get_valid_template_key_from_feedentry.side_effect = lambda _: 'valid_template_key'

    processor._save_to_sqlite_state({'transaction_context': 'context_01_1'})
    processor._save_to_sqlite_contexts(
        context_id='context_01_1', starting_page='1', last_event_id='1',
        last_processed_event=datetime.datetime.strftime(datetime.datetime.utcnow(), processor.dt_format))
    processor._save_to_sqlite_contexts_assets(context_id='context_01_1', append='asset-uuid-0001')

    # function to test
    processor.process_all_entries(entries_to_process=[
        EntryObject(id='id', updated=datetime.datetime.utcnow(), content=ContentObject(value=AtomValueObject(
            _type='ASSET_KENMERK_EIGENSCHAP_VALUES_UPDATED', _typeVersion=1,
            aggregateId=AggregateIdObject(uuid='asset-uuid-0002'))))])

    # assertions
    assert processor.state_db == {'event_id': 'id', 'transaction_context': 'context_01_1'}
    conn = sqlite3.connect(THIS_FOLDER / 'used_sqlite.db')
    c = conn.cursor()
    c.execute('''SELECT id, starting_page, last_event_id FROM contexts''')
    contexts_row = c.fetchone()
    c.execute('''SELECT context_id, asset_uuid FROM contexts_assets''')
    contexts_assets_rows = [row for row in c.fetchall()]
    conn.commit()
    conn.close()
    assert contexts_row == ('context_01_1', '1', '1')
    assert contexts_assets_rows == [('context_01_1', 'asset-uuid-0001')]


def test_process_all_entries_transaction_context_process_entry_without_context_last_processed_too_long_ago():
    # setup
    _, processor = create_processor_unittest_sqlite('used_sqlite.db')
    processor.process_complex_template_using_transaction = Mock()
    processor.get_valid_template_key_from_feedentry = Mock()
    processor.get_valid_template_key_from_feedentry.side_effect = lambda _: 'valid_template_key'

    processor._save_to_sqlite_state({'transaction_context': 'context_01_1'})
    processor._save_to_sqlite_contexts(
        context_id='context_01_1', starting_page='1', last_event_id='1',
        last_processed_event=datetime.datetime.strftime(datetime.datetime(2023, 2, 1, 1, 2, 3), processor.dt_format))
    processor._save_to_sqlite_contexts_assets(context_id='context_01_1', append='asset-uuid-0001')

    # function to test
    processor.process_all_entries(entries_to_process=[
        EntryObject(id='id', updated=datetime.datetime(2023, 2, 1, 1, 3, 4),
                    content=ContentObject(value=AtomValueObject(
                        _type='ASSET_KENMERK_EIGENSCHAP_VALUES_UPDATED', _typeVersion=1,
                        aggregateId=AggregateIdObject(uuid='asset-uuid-0002'))))])

    # assertions
    assert processor.state_db == {'transaction_context': 'context_01_1'}
    conn = sqlite3.connect(THIS_FOLDER / 'used_sqlite.db')
    c = conn.cursor()
    c.execute('''SELECT id, starting_page, last_event_id FROM contexts''')
    contexts_row = c.fetchone()
    c.execute('''SELECT context_id, asset_uuid FROM contexts_assets''')
    contexts_assets_rows = [row for row in c.fetchall()]
    conn.commit()
    conn.close()
    assert contexts_row == ('context_01_1', '1', '1')
    assert contexts_assets_rows == [('context_01_1', 'asset-uuid-0001')]
    assert processor.process_complex_template_using_transaction.called


def test_process_all_entries_no_transaction_context_complex_template_no_context():
    # setup
    _, processor = create_processor_unittest_sqlite('used_sqlite.db')
    processor.process_complex_template_without_context = Mock()
    processor.process_complex_template_without_context.side_effect = lambda asset_uuid, event_id: None
    processor.get_valid_template_key_from_feedentry = Mock()
    processor.get_valid_template_key_from_feedentry.side_effect = lambda _: 'valid_template_key'
    processor.determine_if_template_is_complex = Mock()
    processor.determine_if_template_is_complex.side_effect = lambda template_key: True
    processor._save_to_sqlite_state({'transaction_context': None})

    # function to test
    processor.process_all_entries(entries_to_process=[
        EntryObject(id='id', content=ContentObject(value=AtomValueObject(
            _type='ASSET_KENMERK_EIGENSCHAP_VALUES_UPDATED', _typeVersion=1,
            aggregateId=AggregateIdObject(uuid='asset-uuid-0002'))))])

    # assertions
    assert processor.state_db == {'event_id': 'id', 'transaction_context': None}
    assert processor.process_complex_template_without_context.called


def test_process_all_entries_no_transaction_context_complex_template_new_context():
    # setup
    _, processor = create_processor_unittest_sqlite('used_sqlite.db')
    processor.get_valid_template_key_from_feedentry = Mock()
    processor.get_valid_template_key_from_feedentry.side_effect = lambda _: 'valid_template_key'
    processor.determine_if_template_is_complex = Mock()
    processor.determine_if_template_is_complex.side_effect = lambda template_key: True

    processor._save_to_sqlite_state({'transaction_context': None, 'page': '2'})
    dt_string = datetime.datetime.strftime(datetime.datetime(2023, 2, 1, 1, 2, 3), processor.dt_format)

    # function to test
    processor.process_all_entries(entries_to_process=[
        EntryObject(id='1', updated=dt_string, content=ContentObject(value=AtomValueObject(
            _type='ASSET_KENMERK_EIGENSCHAP_VALUES_UPDATED', _typeVersion=1, contextId='context_01',
            aggregateId=AggregateIdObject(uuid='asset-uuid-0001'))))])

    # assertions
    assert processor.state_db == {'event_id': '1', 'page': '2', 'transaction_context': 'context_01_1'}
    conn = sqlite3.connect(THIS_FOLDER / 'used_sqlite.db')
    c = conn.cursor()
    c.execute('''SELECT id, starting_page, last_event_id, last_processed_event FROM contexts''')
    contexts_row = c.fetchone()
    c.execute('''SELECT context_id, asset_uuid FROM contexts_assets''')
    contexts_assets_rows = [row for row in c.fetchall()]
    conn.commit()
    conn.close()
    assert contexts_row == ('context_01_1', '2', '1', dt_string)
    assert contexts_assets_rows == [('context_01_1', 'asset-uuid-0001')]


def test_process_all_entries_no_transaction_context_complex_template_existing_context_identical_id():
    # setup
    _, processor = create_processor_unittest_sqlite('used_sqlite.db')
    processor.get_valid_template_key_from_feedentry = Mock()
    processor.get_valid_template_key_from_feedentry.side_effect = lambda _: 'valid_template_key'
    processor.determine_if_template_is_complex = Mock()
    processor.determine_if_template_is_complex.side_effect = lambda template_key: True

    processor._save_to_sqlite_state({'transaction_context': None, 'page': '2'})
    processor._save_to_sqlite_contexts(context_id='context_01_1', starting_page='2', last_event_id='1')
    processor._save_to_sqlite_contexts_assets(context_id='context_01_1', append='asset-uuid-0001')

    # function to test
    processor.process_all_entries(entries_to_process=[
        EntryObject(id='1', content=ContentObject(value=AtomValueObject(
            _type='ASSET_KENMERK_EIGENSCHAP_VALUES_UPDATED', _typeVersion=1, contextId='context_01',
            aggregateId=AggregateIdObject(uuid='asset-uuid-0001'))))])

    # assertions
    assert processor.state_db['event_id'] == '1'


def test_process_all_entries_no_transaction_context_complex_template_existing_context_already_processed():
    # setup
    _, processor = create_processor_unittest_sqlite('used_sqlite.db')
    processor.get_valid_template_key_from_feedentry = Mock()
    processor.get_valid_template_key_from_feedentry.side_effect = lambda _: 'valid_template_key'
    processor.determine_if_template_is_complex = Mock()
    processor.determine_if_template_is_complex.side_effect = lambda template_key: True

    processor._save_to_sqlite_state({'transaction_context': None, 'page': '2'})
    processor._save_to_sqlite_contexts(context_id='context_01_1', starting_page='2', last_event_id='2')
    processor._save_to_sqlite_contexts_assets(context_id='context_01_1', append='asset-uuid-0001')
    processor._save_to_sqlite_contexts_assets(context_id='context_01_1', append='asset-uuid-0002')

    # function to test
    processor.process_all_entries(entries_to_process=[
        EntryObject(id='2', content=ContentObject(value=AtomValueObject(
            _type='ASSET_KENMERK_EIGENSCHAP_VALUES_UPDATED', _typeVersion=1, contextId='context_01',
            aggregateId=AggregateIdObject(uuid='asset-uuid-0002'))))])

    # assertions
    assert processor.state_db['event_id'] == '2'


def test_process_all_entries_no_transaction_context_complex_template_existing_context_start_new_context():
    # setup
    _, processor = create_processor_unittest_sqlite('used_sqlite.db')
    processor.get_valid_template_key_from_feedentry = Mock()
    processor.get_valid_template_key_from_feedentry.side_effect = lambda _: 'valid_template_key'
    processor.determine_if_template_is_complex = Mock()
    processor.determine_if_template_is_complex.side_effect = lambda template_key: True
    dt_string = datetime.datetime.strftime(datetime.datetime(2023, 2, 1, 1, 2, 3), processor.dt_format)

    processor._save_to_sqlite_state({'transaction_context': None, 'page': '2'})
    processor._save_to_sqlite_contexts(context_id='context_01_1', starting_page='2', last_event_id='2')
    processor._save_to_sqlite_contexts_assets(context_id='context_01_1', append='asset-uuid-0001')
    processor._save_to_sqlite_contexts_assets(context_id='context_01_1', append='asset-uuid-0002')

    # function to test
    processor.process_all_entries(entries_to_process=[
        EntryObject(id='4', updated=datetime.datetime(2023, 2, 1, 1, 2, 3), content=ContentObject(value=AtomValueObject(
            _type='ASSET_KENMERK_EIGENSCHAP_VALUES_UPDATED', _typeVersion=1, contextId='context_01',
            aggregateId=AggregateIdObject(uuid='asset-uuid-0004'))))])

    # assertions
    assert processor.state_db == {'event_id': '4', 'page': '2', 'transaction_context': 'context_01_4'}
    conn = sqlite3.connect(THIS_FOLDER / 'used_sqlite.db')
    c = conn.cursor()
    c.execute('''SELECT id, starting_page, last_event_id, last_processed_event FROM contexts''')
    contexts_rows = [row for row in c.fetchall()]
    c.execute('''SELECT context_id, asset_uuid FROM contexts_assets''')
    contexts_assets_rows = [row for row in c.fetchall()]
    conn.commit()
    conn.close()
    assert contexts_rows == [('context_01_1', '2', '2', None), ('context_01_4', '2', '4', dt_string)]
    assert contexts_assets_rows == [('context_01_1', 'asset-uuid-0001'), ('context_01_1', 'asset-uuid-0002'),
                                    ('context_01_4', 'asset-uuid-0004')]


def test_process_loop_events_same_context_across_multiple_pages():
    rest_client, processor = create_processor_unittest_sqlite('used_sqlite.db')
    processor._save_to_sqlite_state({'page': '30', 'event_id': '3098'})
    processor.postenmapping_dict = {'valid_template_key': {}}

    def exit_loop():
        raise StopIteration

    processor.wait_seconds = Mock()
    processor.wait_seconds.side_effect = exit_loop
    processor.process_complex_template_using_transaction = Mock()
    processor.process_complex_template_using_transaction.side_effect = exit_loop
    processor.determine_if_template_is_complex = Mock()
    processor.determine_if_template_is_complex.side_effect = lambda template_key: True
    rest_client.get_feedpage = Mock()
    rest_client.get_feedpage.side_effect = return_fake_feedpage_without_new_entries

    processor.process()

    assert processor.process_complex_template_using_transaction.called
    assert processor.state_db['page'] == '31'
    conn = sqlite3.connect(THIS_FOLDER / 'used_sqlite.db')
    c = conn.cursor()
    c.execute('''SELECT id, starting_page, last_event_id, last_processed_event FROM contexts''')
    contexts_rows = [row for row in c.fetchall()]
    c.execute('''SELECT context_id, asset_uuid FROM contexts_assets''')
    contexts_assets_rows = [row for row in c.fetchall()]
    conn.commit()
    conn.close()
    assert contexts_rows == [('context01_3099', '30', '3101', '2023-02-01 01:02:04')]
    assert contexts_assets_rows == [('context01_3099', '0001'), ('context01_3099', '0002')]
