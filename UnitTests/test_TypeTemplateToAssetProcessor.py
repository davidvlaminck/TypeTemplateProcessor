from pathlib import Path
from unittest.mock import Mock, call

from EMInfraRestClient import EMInfraRestClient
from TypeTemplateToAssetProcessor import TypeTemplateToAssetProcessor
from UnitTests.TestObjects.FakeEntryObject import fake_entry_object_with_valid_key, fake_entry_object_without_to, \
    fake_entry_object_with_two_valid_keys, fake_entry_object_without_valid_key
from UnitTests.TestObjects.FakeFeedPage import fake_feedpage_empty_entries
from UnitTests.TestObjects.FakeKenmerkEigenschapValueDTOList import fake_attribute_list, return_fake_attribute_list, \
    fake_attribute_list2


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
