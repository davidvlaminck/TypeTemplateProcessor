import logging
import shelve
import time
from pathlib import Path
from typing import List, Optional, Dict

from otlmow_davie.DavieClient import DavieClient
from otlmow_davie.Enums import AuthenticationType, Environment

from EMInfraDomain import KenmerkEigenschapValueUpdateDTO, ResourceRefDTO, EigenschapTypedValueDTO, \
    ListUpdateDTOKenmerkEigenschapValueUpdateDTO, FeedPage, EntryObject, KenmerkEigenschapValueDTO, \
    KenmerkEigenschapValueDTOList
from EMInfraRestClient import EMInfraRestClient
from PostenMappingDict import PostenMappingDict
from InvalidTemplateKeyError import InvalidTemplateKeyError
from RequestHandler import RequestHandler
from RequesterFactory import RequesterFactory
from SettingsManager import SettingsManager


class TypeTemplateToAssetProcessor:
    def __init__(self, shelve_path: Path, settings_path: Path, auth_type: AuthenticationType, environment: Environment,
                 postenmapping_path: Path):
        self.shelve_path: Path = shelve_path

        self.postenmapping_dict: Dict = PostenMappingDict.mapping_dict

        self._create_rest_client_based_on_settings(auth_type, environment, settings_path)

        self._settings_path = settings_path
        self._auth_type = auth_type
        self._environment = environment

    def _create_rest_client_based_on_settings(self, auth_type, environment, settings_path):
        settings_manager = SettingsManager(settings_path)
        requester = RequesterFactory.create_requester(settings=settings_manager.settings, auth_type=auth_type,
                                                      environment=environment)
        request_handler = RequestHandler(requester)
        self.rest_client: EMInfraRestClient = EMInfraRestClient(request_handler=request_handler)

    def process(self):
        while True:
            if not Path.is_file(self.shelve_path):
                try:
                    import dbm.ndbm
                    with dbm.ndbm.open(str(self.shelve_path), 'c'):
                        pass
                except ModuleNotFoundError:
                    with shelve.open(str(self.shelve_path)):
                        pass
            try:
                self.process_loop()
            except ConnectionError as exc:
                logging.error(str(exc))
                self.wait_seconds(60)
            except StopIteration:
                break
            except Exception as exc:
                print(type(exc))
                logging.error(str(exc))
                self.wait_seconds(60)

    def process_loop(self):
        with shelve.open(str(self.shelve_path), writeback=True) as db:
            if 'event_id' not in db:
                self.save_last_event()
            if 'transaction_context' not in db:
                db['transaction_context'] = None

            while True:
                current_page = self.rest_client.get_feedpage(page=str(db['page']))
                entries_to_process = self.get_entries_to_process(current_page, db['event_id'])
                if len(entries_to_process) == 0:
                    previous_link = next((link for link in current_page.links if link.rel == 'previous'), None)
                    if previous_link is None:
                        logging.info('No events to process, trying again in 10 seconds.')
                        self.wait_seconds()
                        continue
                    else:
                        logging.info(f"Done processing page {db['page']}. Going to the next.")
                        db['page'] = previous_link.href.split('/')[1]
                        continue

                self.process_all_entries(db, entries_to_process)

    def process_all_entries(self, db, entries_to_process: List[EntryObject]):
        for entry in entries_to_process:
            start = time.time()
            if entry.content.value.type != 'ASSET_KENMERK_EIGENSCHAP_VALUES_UPDATED':
                db['event_id'] = entry.id
                continue

            template_key = self.get_valid_template_key_from_feedentry(entry)
            if template_key is None:
                db['event_id'] = entry.id
                continue
            asset_uuid = entry.content.value.aggregateId.uuid

            # determine if a template is single or multiple
            is_complex_template = self.determine_if_template_is_complex(template_key=template_key)

            if is_complex_template:
                # if template is complex, determine if the entry has a context
                # if it has => open a transaction context and process until there a no more
                #   keep track of all the assets in this context with a valid template key.
                #   combine those into a single file for DAVIE to upload
                #   resume with the next event after starting the transaction and skip the assets already processed in this context.

                # if doesn't have a context (DAVIE was not used), apply the template by using a DAVIE upload
                context_id = entry.content.value.contextId
                if context_id is None:
                    self.process_complex_template_using_single_upload(asset_uuid=asset_uuid,
                                                                      template_key=template_key)
                    db['event_id'] = entry.id
                    end = time.time()
                    logging.info(f'processed type_template in {round(end - start, 2)} seconds')
                    continue
                else:
                    db['transaction_context'] = context_id
                    db[context_id] = {'asset_uuids': [], 'event_id': entry.id, 'last_processed_event': entry.updated}
                    self.process_complex_template_using_transaction(template_key=template_key)
                    continue

            # only valid for a 'single' template
            ns, attribute_values = self.get_current_attribute_values(asset_uuid=asset_uuid,
                                                                     template_key=template_key)
            update_dto = self.create_update_dto(template_key=template_key, attribute_values=attribute_values)
            if update_dto is None:
                logging.info(f'Asset {asset_uuid} does not longer need an update by a type template.')
                db['event_id'] = entry.id
                continue

            self.rest_client.patch_eigenschapwaarden(ns=ns, uuid=asset_uuid, update_dto=update_dto)

            db['event_id'] = entry.id
            end = time.time()
            logging.info(f'processed type_template in {round(end - start, 2)} seconds')

    @staticmethod
    def wait_seconds(seconds: int = 10):
        time.sleep(seconds)

    def get_current_attribute_values(self, asset_uuid: str, template_key: str) -> (str, KenmerkEigenschapValueDTOList):
        template = self.postenmapping_dict[template_key]
        assettype_uri = list(template.keys())[0]
        ns = 'onderdeel'
        if 'onderdeel' not in assettype_uri:
            ns = 'installatie'
        return ns, self.rest_client.get_eigenschapwaarden(ns=ns, uuid=asset_uuid)

    def _save_to_shelf(self, entries: Dict) -> None:
        with shelve.open(str(self.shelve_path)) as db:
            for k, v in entries.items():
                db[k] = v

    def save_last_event(self):
        last_page = self.rest_client.get_current_feedpage()
        sorted_entries = sorted(last_page.entries, key=lambda x: x.id, reverse=True)
        self_link = next(self_link for self_link in last_page.links if self_link.rel == 'self')
        self._save_to_shelf(entries={'event_id': sorted_entries[0].id, 'page': self_link.href.split('/')[1]})

    @staticmethod
    def get_entries_to_process(current_page: FeedPage, event_id: str) -> List[EntryObject]:
        event_id_int = int(event_id)
        return list(sorted(filter(lambda x: int(x.id) > event_id_int, current_page.entries), key=lambda x: int(x.id)))

    def get_valid_template_key_from_feedentry(self, entry: EntryObject) -> Optional[str]:
        value_to = entry.content.value.to
        try:
            postnummer_kenmerk = value_to['values']['21164e07-2648-4580-b7f3-f0e291fbf6df']
        except (TypeError, KeyError):
            return None

        valid_postnummers = [p['value'] for p in postnummer_kenmerk['values']
                             if 'value' in p and p['value'] in self.postenmapping_dict]

        if len(valid_postnummers) == 1:
            return valid_postnummers[0]
        return None

    def create_clean_bestekpost_nummer_eig(self, eigenschap_waarde: KenmerkEigenschapValueDTO,
                                           template_key: str) -> KenmerkEigenschapValueUpdateDTO:
        created_eig = KenmerkEigenschapValueUpdateDTO()
        created_eig.eigenschap = ResourceRefDTO()
        created_eig.eigenschap.uuid = eigenschap_waarde.eigenschap.uuid
        created_eig.kenmerkType = ResourceRefDTO()
        created_eig.kenmerkType.uuid = eigenschap_waarde.kenmerkType.uuid
        created_eig.typedValue = EigenschapTypedValueDTO()

        value = eigenschap_waarde.typedValue.value
        if eigenschap_waarde.typedValue.type == 'text':
            if value == template_key:
                created_eig.typedValue.value = None
                created_eig.typedValue.type = 'text'
                return created_eig
            else:
                raise InvalidTemplateKeyError(f'template key {template_key} missing in bestekpostnummer"{value}"')
        elif eigenschap_waarde.typedValue.type == 'list':
            eigenschap_waarde.typedValue.value = list(eigenschap_waarde.typedValue.value)
            valid_template_keys = []
            for value_item in list(eigenschap_waarde.typedValue.value):
                possible_template_key = value_item['value']
                if possible_template_key in self.postenmapping_dict:
                    valid_template_keys.append(possible_template_key)
                    eigenschap_waarde.typedValue.value.remove(value_item)
            if len(valid_template_keys) != 1:
                raise InvalidTemplateKeyError(f'found {len(valid_template_keys)} valid template keys, expected 1. '
                                              f'valid_template_keys: {valid_template_keys}')
            created_eig.typedValue.type = 'list'
            created_eig.typedValue.value = eigenschap_waarde.typedValue.value
            return created_eig
        else:
            raise NotImplementedError('invalid type found in bestekpostnummer')

    def create_update_dto(self, template_key: str, attribute_values: KenmerkEigenschapValueDTOList
                          ) -> Optional[ListUpdateDTOKenmerkEigenschapValueUpdateDTO]:
        template = self.postenmapping_dict[template_key]
        eigenschap_waarden = attribute_values
        assettype_uri = list(template.keys())[0]
        attributen_to_process = list(template[assettype_uri]['attributen'].values())
        attribuut_uris = list(map(lambda x: x['typeURI'], attributen_to_process))
        nieuwe_eig = []
        kenmerktype_uuid = ''
        if 'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMObject.bestekPostNummer' not in list(
                map(lambda x: x.eigenschap.uri, eigenschap_waarden.data)):
            return None
        for eigenschap_waarde in list(eigenschap_waarden.data):
            if eigenschap_waarde.eigenschap.uri not in attribuut_uris:
                if eigenschap_waarde.eigenschap.uri == \
                        'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMObject.bestekPostNummer':
                    kenmerktype_uuid = eigenschap_waarde.kenmerkType.uuid
                    try:
                        nieuwe_eig = [self.create_clean_bestekpost_nummer_eig(eigenschap_waarde=eigenschap_waarde,
                                                                              template_key=template_key)]
                    except InvalidTemplateKeyError:
                        return None
                eigenschap_waarden.data.remove(eigenschap_waarde)
            else:
                attribuut = next(
                    x for x in attributen_to_process if x['typeURI'] == eigenschap_waarde.eigenschap.uri)
                attributen_to_process.remove(attribuut)
        for attribuut in attributen_to_process:
            if attribuut[
                'typeURI'] == 'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMObject.typeURI' or \
                    attribuut[
                        'typeURI'] == 'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMDBStatus.isActief':
                continue
            eig = self.rest_client.get_eigenschap_by_uri(uri=attribuut['typeURI'])
            if '.' in attribuut['dotnotation']:
                raise NotImplementedError('complex datatypes not yet implemented')
            datatype = eig.type.datatype.type.type
            created_eig = KenmerkEigenschapValueUpdateDTO()
            created_eig.eigenschap = ResourceRefDTO()
            created_eig.eigenschap.uuid = eig.uuid
            created_eig.kenmerkType = ResourceRefDTO()
            created_eig.kenmerkType.uuid = kenmerktype_uuid
            nieuwe_eig.append(created_eig)

            created_eig.typedValue = EigenschapTypedValueDTO()

            if datatype == 'text' or datatype == 'keuzelijst':
                created_eig.typedValue.type = 'text'
                created_eig.typedValue.value = attribuut['value']
            elif datatype == 'number':
                created_eig.typedValue.type = 'number'
                created_eig.typedValue.value = float(attribuut['value'])
                int_value = int(attribuut['value'])
                if int_value == created_eig.typedValue.value:
                    created_eig.typedValue.value = int_value
            elif datatype == 'boolean':
                created_eig.typedValue.type = 'boolean'
                created_eig.typedValue.value = bool(attribuut['value'])
            else:
                raise NotImplementedError(f'no implementation yet for {datatype}')

        update_dto = ListUpdateDTOKenmerkEigenschapValueUpdateDTO()
        update_dto.data = nieuwe_eig
        return update_dto

    def determine_if_template_is_complex(self, template_key):
        template = self.postenmapping_dict[template_key]
        return len(template.keys()) > 1

    def process_complex_template_using_single_upload(self, event_id: str, asset_uuid: str, template_key: str) -> None:
        davie_client = DavieClient(settings_path=self._settings_path,
                                   auth_type=self._auth_type,
                                   environment=self._environment)

        self._save_to_shelf({'single_upload': event_id})

        aanlevering = davie_client.create_aanlevering_employee(
            niveau='LOG-1', referentie='b2b integratie test 2',
            verificatorId='6c2b7c0a-11a9-443a-a96b-a1bec249c629')
        davie_client.upload_file(id=aanlevering.id,
                                 file_path=Path('type_template_2_buizen.json'))
        davie_client.finalize_and_wait(id=aanlevering.id)
