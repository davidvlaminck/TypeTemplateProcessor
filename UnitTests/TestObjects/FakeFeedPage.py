from EMInfraDomain import FeedPage, Link, EntryObject

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
