import datetime

from EMInfraDomain import FeedPage, Link, EntryObject, ContentObject, AtomValueObject, AggregateIdObject

fake_feedpage_empty_entries = FeedPage(id='EM-Infra change feed', base='/feed', title='EM-Infra change feed',
                                       updated='2023-01-01T00:00:00.000+01:00')
fake_feedpage_empty_entries.links = [
    Link(rel='self', href='/10/100'),
    Link(rel='last', href='/0/100'),
    Link(rel='next', href='/9/100')]
fake_feedpage_empty_entries.entries = [
    EntryObject(id='1011'),
    EntryObject(id='1010'),
    EntryObject(id='1009')]

fake_feedpage_without_new_entries = FeedPage(id='EM-Infra change feed', base='/feed', title='EM-Infra change feed',
                                             updated='2023-01-01T00:00:00.000+01:00')
fake_feedpage_without_new_entries.links = [
    Link(rel='self', href='/10/100'),
    Link(rel='last', href='/0/100'),
    Link(rel='next', href='/9/100')]
fake_feedpage_without_new_entries.entries = [
    EntryObject(id='1010'),
    EntryObject(id='1009')]

fake_feedpage_with_new_entries_1 = FeedPage(id='EM-Infra change feed', base='/feed', title='EM-Infra change feed',
                                            updated='2023-01-01T00:00:00.000+01:00')
fake_feedpage_with_new_entries_1.links = [
    Link(rel='self', href='/20/100'),
    Link(rel='previous', href='/21/100'),
    Link(rel='last', href='/0/100'),
    Link(rel='next', href='/19/100')]
fake_feedpage_with_new_entries_1.entries = [
    EntryObject(id='1010'),
    EntryObject(id='1009')]

fake_feedpage_with_new_entries_2 = FeedPage(id='EM-Infra change feed', base='/feed', title='EM-Infra change feed',
                                            updated='2023-01-01T00:00:00.000+01:00')
fake_feedpage_with_new_entries_2.links = [
    Link(rel='self', href='/21/100'),
    Link(rel='last', href='/0/100'),
    Link(rel='next', href='/20/100')]
fake_feedpage_with_new_entries_2.entries = []

fake_feedpage_with_same_context_1 = FeedPage(id='EM-Infra change feed', base='/feed', title='EM-Infra change feed',
                                             updated='2023-01-01T00:00:00.000+01:00')
fake_feedpage_with_same_context_1.links = [
    Link(rel='self', href='/30/100'),
    Link(rel='previous', href='/31/100'),
    Link(rel='last', href='/0/100'),
    Link(rel='next', href='/29/100')]
fake_feedpage_with_same_context_1.entries = [
    EntryObject(id='3100', content=ContentObject(
        value=AtomValueObject(_type='SOME_OTHER_TYPE', _typeVersion=1))),
    EntryObject(id='3099', updated=datetime.datetime(2023, 2, 1, 1, 2, 3), content=ContentObject(
        value=AtomValueObject(_type='ASSET_KENMERK_EIGENSCHAP_VALUES_UPDATED',
                              _typeVersion=1,
                              contextId='context01',
                              aggregateId=AggregateIdObject(uuid='0001'),
                              to={'values': {
                                  '21164e07-2648-4580-b7f3-f0e291fbf6df': {
                                      'values': [{'dataType': 'TEXT', 'value': 'valid_template_key'}]}}})))]

fake_feedpage_with_same_context_2 = FeedPage(id='EM-Infra change feed', base='/feed', title='EM-Infra change feed',
                                             updated='2023-01-01T00:00:00.000+01:00')
fake_feedpage_with_same_context_2.links = [
    Link(rel='self', href='/31/100'),
    Link(rel='last', href='/0/100'),
    Link(rel='next', href='/30/100')]
fake_feedpage_with_same_context_2.entries = [
    EntryObject(id='3101', updated=datetime.datetime(2023, 2, 1, 1, 2, 4), content=ContentObject(
        value=AtomValueObject(_type='ASSET_KENMERK_EIGENSCHAP_VALUES_UPDATED',
                              _typeVersion=1,
                              contextId='context01',
                              aggregateId=AggregateIdObject(uuid='0002'),
                              to={'values': {
                                  '21164e07-2648-4580-b7f3-f0e291fbf6df': {
                                      'values': [{'dataType': 'TEXT', 'value': 'valid_template_key'}]}}})))]


def return_fake_feedpage_without_new_entries(*args, **kwargs):
    if kwargs['page'] == '10':
        return fake_feedpage_without_new_entries
    elif kwargs['page'] == '20':
        return fake_feedpage_with_new_entries_1
    elif kwargs['page'] == '21':
        return fake_feedpage_with_new_entries_2
    elif kwargs['page'] == '30':
        return fake_feedpage_with_same_context_1
    elif kwargs['page'] == '31':
        return fake_feedpage_with_same_context_2
