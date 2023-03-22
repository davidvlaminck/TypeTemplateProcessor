from typing import Optional


class ZoekParameterPayload:
    def __init__(self, size: int = 100, from_: int = None, from_cursor: Optional[str] = None, paging_mode: str = "CURSOR",
                 expansions:  Optional[dict] = None, settings: Optional[dict] = None, selection: Optional[dict] = None):
        if expansions is None:
            expansions = {}
        if settings is None:
            settings = {}
        if selection is None:
            selection = {}
        self.size = size
        self.from_ = from_
        self.from_cursor = from_cursor
        self.paging_mode = paging_mode
        self.expansions = expansions
        self.settings = settings
        self.selection = selection

    def add_term(self, logical_op: str = 'AND', property: str = '', value=None, operator: str = '', negate: bool = None):
        if 'expressions' not in self.selection:
            self.selection['expressions'] = []
            self.selection['expressions'].append({"logicalOp": None, 'terms': []})

        term = {}
        if logical_op == 'AND':
            term['logicalOp'] = 'AND'
        if property != '':
            term['property'] = property
        if value is not None:
            term['value'] = value
        if operator != '':
            term['operator'] = operator
        if negate is not None:
            term['negate'] = negate

        if len(self.selection['expressions'][0]['terms']) == 0:
            term['logicalOp'] = None
        self.selection['expressions'][0]['terms'].append(term)

    def fill_dict(self):
        if self.paging_mode == 'OFFSET' and self.from_ is None:
            self.from_ = 0

        d = {}
        d['size'] = self.size
        d['from'] = self.from_
        d['fromCursor'] = self.from_cursor
        d['pagingMode'] = self.paging_mode
        d['expansions'] = self.expansions
        d['settings'] = self.settings
        d['selection'] = self.selection

        return d
