import functools
import json
import time

from EMInfraDomain import FeedPage, ListUpdateDTOKenmerkEigenschapValueUpdateDTO, KenmerkEigenschapValueDTOList, \
    EigenschapDTOList, EigenschapDTO


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

    def import_assets_from_webservice_by_uuids(self, asset_uuids: [str]) -> [dict]:
        asset_list_string = '", "'.join(asset_uuids)
        filter_string = '{ "uuid": ' + f'["{asset_list_string}"]' + ' }'
        return self.get_objects_from_oslo_search_endpoint(url_part='assets', filter_string=filter_string)

    def get_objects_from_oslo_search_endpoint(self, url_part: str,
                                              filter_string: str = '{}', size: int = 100, contact_info: bool = False,
                                              expansions_string: str = '{}') -> [dict]:
        paging_cursor = ''
        url = f'core/api/otl/{url_part}/search'
        body_fixed_part = '{"size": ' + f'{size}' + ''
        if filter_string != '{}':
            body_fixed_part += ', "filters": ' + filter_string
        if expansions_string != '{}':
            body_fixed_part += ', "expansions": ' + expansions_string

        while True:
            body = body_fixed_part
            if paging_cursor != '':
                body += ', "fromCursor": ' + f'"{paging_cursor}"'
            body += '}'
            json_data = json.loads(body)

            response = self.request_handler.perform_post_request(url=url, json_data=json_data)

            decoded_string = response.content.decode("utf-8")
            dict_obj = json.loads(decoded_string)
            keys = response.headers.keys()
            yield from dict_obj['@graph']
            if 'em-paging-next-cursor' in keys:
                paging_cursor = response.headers['em-paging-next-cursor']
            else:
                break
