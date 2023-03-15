import functools
import json
import time
from typing import Generator

from EMInfraDomain import FeedPage, ListUpdateDTOKenmerkEigenschapValueUpdateDTO, KenmerkEigenschapValueDTOList, \
    EigenschapDTOList, EigenschapDTO
from ZoekParameterPayload import ZoekParameterPayload


class EMInfraRestClient:
    def __init__(self, request_handler):
        self.request_handler = request_handler
        self.request_handler.requester.first_part_url += 'eminfra/'
        self.pagingcursor = ''

    def get_feedpage_by_name(self, resource: str):
        response = self.request_handler.perform_post_request(url=f'feedproxy/feed/{resource}/0/100')
        if response.status_code != 200:
            print(response)
            raise ProcessLookupError(response.content.decode("utf-8"))
        return json.loads(response.content.decode("utf-8"))

    def search_historiek(self, zoek_payload: ZoekParameterPayload = None) -> Generator:
        url = f'core/api/events/search'
        current_count = 0
        current_paging_cursor = ''
        while True:
            if zoek_payload.paging_mode == 'CURSOR' and current_paging_cursor != '':
                zoek_payload.from_cursor = current_paging_cursor

            json_data = zoek_payload.fill_dict()
            json_data = json.dumps(json_data, indent=0)
            response = self.request_handler.perform_post_request(url=url, data=json_data)

            decoded_string = response.content.decode("utf-8")
            dict_obj = json.loads(decoded_string)

            yield from dict_obj['data']

            if zoek_payload.paging_mode == 'CURSOR':
                if 'next' in dict_obj:
                    current_paging_cursor = dict_obj['next']
                else:
                    current_paging_cursor = ''

                if current_paging_cursor == '':
                    return
            elif zoek_payload.paging_mode == 'OFFSET':
                current_count += len(dict_obj['data'])
                if current_count == dict_obj['totalCount']:
                    return
                zoek_payload.from_ += zoek_payload.size

    def get_all_installaties_by_zoek_parameter(self, zoek_payload: ZoekParameterPayload = None) -> Generator:
        url = f'core/api/installaties/search'

        current_count = 0
        current_paging_cursor = ''
        while True:
            if zoek_payload.paging_mode == 'CURSOR' and current_paging_cursor != '':
                zoek_payload.from_cursor = current_paging_cursor

            json_data = zoek_payload.fill_dict()
            json_data = json.dumps(json_data, indent=0)
            response = self.request_handler.perform_post_request(url=url, data=json_data)

            decoded_string = response.content.decode("utf-8")
            dict_obj = json.loads(decoded_string)

            yield from dict_obj['data']

            if zoek_payload.paging_mode == 'CURSOR':
                if 'next' in dict_obj:
                    current_paging_cursor = dict_obj['next']
                else:
                    current_paging_cursor = ''

                if current_paging_cursor == '':
                    return
            elif zoek_payload.paging_mode == 'OFFSET':
                current_count += len(dict_obj['data'])
                if current_count == dict_obj['totalCount']:
                    return
                zoek_payload.from_ += zoek_payload.size

    def update_bestekref(self, bestek_ref, type_bestekref: str):
        bestek_ref['type'] = type_bestekref
        uuid = bestek_ref.pop('uuid', None)
        response = self.request_handler.perform_put_request(
            url=f'core/api/bestekrefs/{uuid}',
            data=json.dumps(bestek_ref))
        if response.status_code != 202:
            print(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

    def update_geometrie_nauwkeurigheid(self, uuid, ns, log_uuid, nk):
        start = time.time()
        ns_uri = 'onderdelen'
        if ns == 'installatie':
            ns_uri = 'installaties'

        if nk == '':
            log_dict = {"nauwkeurigheid": None}
        else:
            log_dict = {"nauwkeurigheid": f"_{nk}"}

        response = self.request_handler.perform_put_request(
            url=f'core/api/{ns_uri}/{uuid}/kenmerken/aabe29e0-9303-45f1-839e-159d70ec2859/logs/{log_uuid}',
            data=json.dumps(log_dict))
        if response.status_code != 202:
            print(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

        end = time.time()
        print(f'changed geometrie in {round(end - start, 2)} seconds')

    def get_geometrie(self, uuid, ns):
        start = time.time()
        ns_uri = 'onderdelen'
        if ns == 'installatie':
            ns_uri = 'installaties'
        response = self.request_handler.perform_get_request(
            url=f'core/api/{ns_uri}/{uuid}/kenmerken/aabe29e0-9303-45f1-839e-159d70ec2859')
        if response.status_code != 200:
            print(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

        response_string = response.content.decode("utf-8")
        json_dict = json.loads(response_string)
        end = time.time()
        print(f'fetched geometrie in {round(end - start, 2)} seconds')
        return json_dict

    @functools.lru_cache(maxsize=None)
    def get_eigenschap_by_uri(self, uri: str) -> EigenschapDTO:
        payload = {
            "size": 1,
            "from": 0,
            "selection": {
                "expressions": [
                    {
                        "terms": [
                            {
                                "property": "uri",
                                "value": uri,
                                "operator": "EQ"
                            }
                        ]
                    }
                ]
            }
        }

        response = self.request_handler.perform_post_request(
            url=f'core/api/eigenschappen/search',
            data=json.dumps(payload))
        if response.status_code != 200:
            print(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

        response_string = response.content.decode("utf-8")
        eigenschap_dto_list = EigenschapDTOList.parse_raw(response_string)
        return eigenschap_dto_list.data[0]

    def patch_eigenschapwaarden(self, uuid: str, update_dto: ListUpdateDTOKenmerkEigenschapValueUpdateDTO, ns: str):
        ns_uri = 'onderdelen'
        if ns == 'installatie':
            ns_uri = 'installaties'
        json_data = update_dto.json().replace('{"type":', '{"_type":')
        response = self.request_handler.perform_patch_request(
            url=f'core/api/{ns_uri}/{uuid}/eigenschapwaarden', data=json_data)
        if response.status_code != 202:
            print(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

    def get_eigenschapwaarden(self, uuid, ns) -> KenmerkEigenschapValueDTOList:
        start = time.time()
        ns_uri = 'onderdelen'
        if ns == 'installatie':
            ns_uri = 'installaties'
        response = self.request_handler.perform_get_request(
            url=f'core/api/{ns_uri}/{uuid}/eigenschapwaarden')
        if response.status_code != 200:
            print(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

        response_string = response.content.decode("utf-8")
        eigenschap_waarden = KenmerkEigenschapValueDTOList.parse_raw(response_string)
        end = time.time()
        print(f'fetched eigenschapwaarden in {round(end - start, 2)} seconds')
        return eigenschap_waarden

    def get_feedpage(self, page: str) -> FeedPage:
        response = self.request_handler.perform_get_request(
            url=f'core/api/feed/{page}/100')
        if response.status_code != 200:
            print(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

        response_string = response.content.decode("utf-8")
        feed_page = FeedPage.parse_raw(response_string)
        return feed_page

    def get_current_feedpage(self) -> FeedPage:
        response = self.request_handler.perform_get_request(
            url='core/api/feed')
        if response.status_code != 200:
            print(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

        response_string = response.content.decode("utf-8")
        feed_page = FeedPage.parse_raw(response_string)
        return feed_page
