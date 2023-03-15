from EMInfraDomain import EntryObject, ContentObject, AtomValueObject

fake_entry_object_with_valid_key = EntryObject(id='16814073')
fake_entry_object_with_valid_key.content = ContentObject()
fake_entry_object_with_valid_key.content.value = AtomValueObject(
    _type='ASSET_KENMERK_EIGENSCHAP_VALUES_UPDATED', _typeVersion='1')
fake_entry_object_with_valid_key.content.value.to = {
    'values': {
        '21164e07-2648-4580-b7f3-f0e291fbf6df': {
            'values': [
                {
                    'dataType': 'TEXT',
                    'value': 'valid_template_key'
                }
            ]
        }
    }
}

fake_entry_object_with_two_valid_keys = EntryObject(id='16814073')
fake_entry_object_with_two_valid_keys.content = ContentObject()
fake_entry_object_with_two_valid_keys.content.value = AtomValueObject(
    _type='ASSET_KENMERK_EIGENSCHAP_VALUES_UPDATED', _typeVersion='1')
fake_entry_object_with_two_valid_keys.content.value.to = {
    'values': {
        '21164e07-2648-4580-b7f3-f0e291fbf6df': {
            'values': [
                {
                    'dataType': 'TEXT',
                    'value': 'valid_template_key'
                },  {
                    'dataType': 'TEXT',
                    'value': 'valid_template_key_2'
                }
            ]
        }
    }
}

fake_entry_object_without_valid_key = EntryObject(id='16814073')
fake_entry_object_without_valid_key.content = ContentObject()
fake_entry_object_without_valid_key.content.value = AtomValueObject(
    _type='ASSET_KENMERK_EIGENSCHAP_VALUES_UPDATED', _typeVersion='1')
fake_entry_object_without_valid_key.content.value.to = {
    'values': {
        '21164e07-2648-4580-b7f3-f0e291fbf6df': {
            'values': [
                {
                    'dataType': 'TEXT',
                    'value': 'invalid_template_key'
                }
            ]
        }
    }
}

fake_entry_object_without_to = EntryObject(id='16814073')
fake_entry_object_without_to.content = ContentObject()
fake_entry_object_without_to.content.value = AtomValueObject(
    _type='ASSET_KENMERK_EIGENSCHAP_VALUES_UPDATED', _typeVersion='1')
