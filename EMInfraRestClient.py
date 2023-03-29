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
