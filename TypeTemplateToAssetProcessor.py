import copy
import datetime
import logging
import os
import sqlite3
import time
from pathlib import Path
from typing import List, Optional, Dict

from otlmow_converter.DotnotationHelper import DotnotationHelper
from otlmow_converter.OtlmowConverter import OtlmowConverter
from otlmow_davie.DavieClient import DavieClient
from otlmow_davie.Enums import AuthenticationType, Environment
from otlmow_model.BaseClasses.OTLObject import OTLObject
from otlmow_model.Helpers.AssetCreator import dynamic_create_instance_from_uri

from EMInfraDecoder import EMInfraDecoder
from EMInfraDomain import KenmerkEigenschapValueUpdateDTO, ResourceRefDTO, EigenschapTypedValueDTO, \
    ListUpdateDTOKenmerkEigenschapValueUpdateDTO, FeedPage, EntryObject, KenmerkEigenschapValueDTO, \
    KenmerkEigenschapValueDTOList
from EMInfraRestClient import EMInfraRestClient
from InvalidTemplateKeyError import InvalidTemplateKeyError
from PostenMappingDict import PostenMappingDict
from RequestHandler import RequestHandler
from RequesterFactory import RequesterFactory
from SettingsManager import SettingsManager


class TypeTemplateToAssetProcessor:
    def __init__(self, sqlite_path: Path, settings_path: Path, auth_type: AuthenticationType, environment: Environment,
                 postenmapping_path: Path):
        self.sqlite_path: Path = sqlite_path
        self.create_sqlite_if_not_exists(sqlite_path)

        self.state_db: dict = {}
        self._load_state_db()

        self.postenmapping_dict: Dict = PostenMappingDict.mapping_dict

        self._create_rest_client_based_on_settings(auth_type, environment, settings_path)

        self._settings_path = settings_path
        self._auth_type = auth_type
        self._environment = environment

        self.dt_format = '%Y-%m-%dT%H:%M:%SZ'

        THIS_DIRECTORY = Path(__file__).parent
        davie_settings_path = Path(THIS_DIRECTORY / 'settings_davie.json')
        shelve_path = Path(THIS_DIRECTORY / 'davie_shelve')
        self._create_davie_client_based_on_settings(auth_type, shelve_path, environment, davie_settings_path)

    def _create_davie_client_based_on_settings(self, auth_type, shelve_path, environment, davie_settings_path):
        self.davie_client = DavieClient(settings_path=davie_settings_path,
                                        shelve_path=shelve_path,
                                        auth_type=auth_type,
                                        environment=environment)

    def _create_rest_client_based_on_settings(self, auth_type, environment, settings_path):
        settings_manager = SettingsManager(settings_path)
        requester = RequesterFactory.create_requester(settings=settings_manager.settings, auth_type=auth_type,
                                                      environment=environment)
        request_handler = RequestHandler(requester)
        self.rest_client: EMInfraRestClient = EMInfraRestClient(request_handler=request_handler)

    def process(self):
        while True:
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
        if 'event_id' not in self.state_db:
            self.save_last_event()
        if 'transaction_context' not in self.state_db:
            self._save_to_sqlite_state({'transaction_context': None})
        # if 'contexts' not in self.state_db:
        #     self._save_to_sqlite_state({'contexts': {}})
        # if 'tracked_aanleveringen' not in self.state_db:
        #     self._save_to_sqlite_state({'tracked_aanleveringen': {}})

        while True:
            try:
                current_page = self.rest_client.get_feedpage(page=str(self.state_db['page']))
            except ProcessLookupError():
                self.wait_seconds(60)
                continue
            entries_to_process = self.get_entries_to_process(current_page, self.state_db['event_id'])
            if len(entries_to_process) == 0 and self.state_db['transaction_context'] is None:
                previous_link = next((link for link in current_page.links if link.rel == 'previous'), None)
                if previous_link is None:
                    logging.info('No events to process, trying again in 10 seconds.')
                    self.wait_seconds()
                    continue
                else:
                    logging.info(f"Done processing page {self.state_db['page']}. Going to the next.")
                    self._save_to_sqlite_state({'page': previous_link.href.split('/')[1]})
                    continue

            self.process_all_entries(entries_to_process)

    def process_all_entries(self, entries_to_process: List[EntryObject]):
        if len(entries_to_process) == 0 and self.state_db['transaction_context'] is not None:
            self.process_complex_template_using_transaction()

        for entry in entries_to_process:
            start = time.time()
            if entry.content.value.type != 'ASSET_KENMERK_EIGENSCHAP_VALUES_UPDATED':
                self._save_to_sqlite_state({'event_id': entry.id})
                continue

            template_key = self.get_valid_template_key_from_feedentry(entry)
            if template_key is None:
                self._save_to_sqlite_state({'event_id': entry.id})
                continue

            asset_uuid = entry.content.value.aggregateId.uuid
            context_id = entry.content.value.contextId

            if self.state_db['transaction_context'] is not None:
                # add any assets with a valid template key if they are using the same context_id
                if context_id is not None and self.state_db['transaction_context'].startswith(context_id):
                    self._save_to_sqlite_contexts(context_id=self.state_db['transaction_context'],
                                                  last_event_id=entry.id, last_processed_event=entry.updated, )
                    self._save_to_sqlite_contexts_assets(context_id=self.state_db['transaction_context'],
                                                         append=asset_uuid)
                else:
                    # has it been too long since we last found assets to add?
                    if self.has_last_processed_been_too_long(current_updated=entry.updated):
                        self.process_complex_template_using_transaction()
                        return

                # change event_id and look for more within the same context
                self._save_to_sqlite_state({'event_id': entry.id})
                continue

            # determine if a template is single or multiple
            is_complex_template = self.determine_if_template_is_complex(template_key=template_key)

            if is_complex_template:
                # if template is complex, determine if the entry has a context
                # if it has => open a transaction context and process until there a no more
                #   keep track of all the assets in this context with a valid template key.
                #   combine those into a single file for DAVIE to upload
                #   resume with the next event after starting the transaction and skip the assets already processed within this context.

                # if doesn't have a context (DAVIE was not used), apply the template by using a DAVIE upload
                if context_id is None:
                    self.process_complex_template_using_single_upload(
                        asset_uuid=asset_uuid, template_key=template_key, event_id=entry.id)
                    self._save_to_sqlite_state({'event_id': entry.id})
                    end = time.time()
                    logging.info(f'processed type_template in {round(end - start, 2)} seconds')
                    continue
                else:
                    # check if we need to start a transaction_context
                    context_entry = context_id + '_' + entry.id
                    context_info = self.get_context_by_context_id(context_entry)
                    if context_info is not None:
                        self._save_to_sqlite_state({'event_id': entry.id})
                        continue  # don't start the same transaction again

                    # check if we have already done this one
                    if self.check_if_already_done(context_uuid=context_id, entry_id=int(entry.id)):
                        self._save_to_sqlite_state({'event_id': entry.id})
                        continue

                    # yes, start a transaction_context

                    self._save_to_sqlite_contexts(
                        context_id=context_entry, last_event_id=entry.id, starting_page=self.state_db['page'],
                        last_processed_event=datetime.datetime.strftime(entry.updated, self.dt_format))
                    self._save_to_sqlite_contexts_assets(context_id=context_entry, append=asset_uuid)

                    self._save_to_sqlite_state({'transaction_context': context_entry,
                                                'event_id': entry.id})
                    continue

            # only valid for a 'single' template
            ns, attribute_values = self.get_current_attribute_values(asset_uuid=asset_uuid,
                                                                     template_key=template_key)
            update_dto = self.create_update_dto(template_key=template_key, attribute_values=attribute_values)
            if update_dto is None:
                logging.info(f'Asset {asset_uuid} does not longer need an update by a type template.')
                self._save_to_sqlite_state({'event_id': entry.id})
                continue

            self.rest_client.patch_eigenschapwaarden(ns=ns, uuid=asset_uuid, update_dto=update_dto)

            self._save_to_sqlite_state({'event_id': entry.id})
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

    def _save_to_sqlite_state(self, entries: Dict) -> None:
        if len(entries.items()) == 0:
            return
        conn = sqlite3.connect(self.sqlite_path)
        c = conn.cursor()

        # get all names in table state
        c.execute('SELECT name FROM state;')
        existing_keys = set(row[0] for row in c.fetchall())
        entries_keys = set(entries.keys())

        # insert
        keys_to_insert = entries_keys - existing_keys
        for key in keys_to_insert:
            c.execute('''INSERT INTO state (name, value) VALUES (?, ?)''', (key, entries[key]))

        keys_to_update = entries_keys - keys_to_insert
        # update
        for key in keys_to_update:
            c.execute('''UPDATE state SET value = ? WHERE name = ?''', (entries[key], key))

        c.execute('SELECT name, value FROM state;')
        self.state_db = {row[0]: row[1] for row in c.fetchall()}

        conn.commit()
        conn.close()

    def _load_state_db(self) -> None:
        conn = sqlite3.connect(self.sqlite_path)
        c = conn.cursor()

        c.execute('SELECT name, value FROM state;')
        self.state_db = {row[0]: row[1] for row in c.fetchall()}

        conn.commit()
        conn.close()

    def save_last_event(self):
        while True:
            try:
                last_page = self.rest_client.get_current_feedpage()
            except ProcessLookupError:
                self.wait_seconds(60)
                continue
            sorted_entries = sorted(last_page.entries, key=lambda x: x.id, reverse=True)
            self_link = next(self_link for self_link in last_page.links if self_link.rel == 'self')
            self._save_to_sqlite_state(entries={'event_id': sorted_entries[0].id, 'page': self_link.href.split('/')[1]})
            break

    @staticmethod
    def get_entries_to_process(current_page: FeedPage, event_id: str) -> List[EntryObject]:
        event_id_int = int(event_id)
        return list(sorted(filter(lambda x: int(x.id) > event_id_int, current_page.entries), key=lambda x: int(x.id)))

    def get_valid_template_key_from_feedentry(self, entry: EntryObject) -> Optional[str]:
        value_to = entry.content.value.to
        try:
            if self._environment == Environment.dev:
                postnummer_kenmerk = value_to['values']['62fc3961-b59b-4f42-a27d-74f50e87130f']
            else:
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

    def perform_davie_aanlevering(self, reference: str, file_path: Path, event_id: str):
        aanlevering = self.get_aanlevering_by_event_id(event_id=event_id)
        if aanlevering is None:
            aanlevering = self.davie_client.create_aanlevering_employee(
                niveau='LOG-1', referentie=reference,
                verificatorId='6c2b7c0a-11a9-443a-a96b-a1bec249c629')

            aanlevering_id = aanlevering.id
            self._save_to_sqlite_aanleveringen(event_id=event_id, aanlevering_id=aanlevering_id, state='created')
            aanlevering = self.get_aanlevering_by_event_id(event_id=event_id)
        else:
            aanlevering_id = aanlevering[1]

        if aanlevering[2] == 'created':
            self.davie_client.upload_file(id=aanlevering_id, file_path=file_path)
            self._save_to_sqlite_aanleveringen(event_id=event_id, state='uploaded')
            aanlevering = self.get_aanlevering_by_event_id(event_id=event_id)

        if aanlevering[2] == 'uploaded':
            self.davie_client.finalize_and_wait(id=aanlevering_id)
            self._save_to_sqlite_aanleveringen(event_id=event_id, state='processed')

    def process_complex_template_using_single_upload(self, event_id: str, asset_uuid: str, template_key: str) -> None:
        self._save_to_shelf({'single_upload': event_id})

        asset_dict = next(self.rest_client.import_assets_from_webservice_by_uuids(asset_uuids=[asset_uuid]))
        object_to_process = EMInfraDecoder().decode_json_object(obj=asset_dict)

        objects_to_upload = []

        valid_postnummers = [postnummer for postnummer in object_to_process.bestekPostNummer
                             if postnummer in self.postenmapping_dict]

        if len(valid_postnummers) != 1:
            return

        objects_to_upload.extend(self.create_assets_from_template(base_asset=object_to_process, asset_index=0,
                                                                  template_key=valid_postnummers[0]))

        converter = OtlmowConverter()
        file_path = Path(f'temp/{event_id}_{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}.json')
        converter.create_file_from_assets(filepath=file_path, list_of_objects=objects_to_upload)

        self.perform_davie_aanlevering(reference=f'type template processor event {event_id}',
                                       event_id=event_id, file_path=file_path)
        os.unlink(file_path)

        existing_aanleveringen = self.state_db['tracked_aanleveringen']
        del existing_aanleveringen[event_id]
        self._save_to_shelf({'tracked_aanleveringen': existing_aanleveringen,
                             'single_upload': None})

    def process_complex_template_using_transaction(self):
        context_entry = self.state_db['transaction_context']
        asset_uuids = self._get_asset_uuids_by_context_id(context_entry)
        asset_dicts = self.rest_client.import_assets_from_webservice_by_uuids(asset_uuids=asset_uuids)
        objects_to_process = [EMInfraDecoder().decode_json_object(asset_dict) for asset_dict in asset_dicts]

        objects_to_upload = []
        for object_nr, object_to_process in enumerate(objects_to_process):
            valid_postnummers = [postnummer for postnummer in object_to_process.bestekPostNummer
                                 if postnummer in self.postenmapping_dict]

            if len(valid_postnummers) != 1:
                continue

            objects_to_upload.extend(self.create_assets_from_template(base_asset=object_to_process,
                                                                      template_key=valid_postnummers[0],
                                                                      asset_index=object_nr))

        converter = OtlmowConverter()

        if not os.path.exists('temp'):
            os.makedirs('temp')
        file_path = Path(f'temp/{context_entry}_{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}.json')
        converter.create_file_from_assets(filepath=file_path, list_of_objects=objects_to_upload)

        event_id = context_entry.split('_')[1]
        self.perform_davie_aanlevering(reference=f'type template processor event {context_entry}',
                                       file_path=file_path, event_id=event_id)
        os.unlink(file_path)

        context = self.get_context_by_context_id(context_id=context_entry)
        self._save_to_sqlite_state({'event_id': event_id, 'transaction_context': None, 'page': context[1]})

    def has_last_processed_been_too_long(self, current_updated: datetime,
                                         max_interval_in_minutes: int = 1) -> bool:
        last_processed = self.get_last_processed_by_context_id(context_id=self.state_db['transaction_context'])
        return last_processed + datetime.timedelta(minutes=max_interval_in_minutes) < current_updated

    def create_assets_from_template(self, template_key: str, base_asset: OTLObject, asset_index: int) -> List[
        OTLObject]:
        mapping = copy.deepcopy(self.postenmapping_dict[template_key])
        copy_base_asset = dynamic_create_instance_from_uri(base_asset.typeURI)
        copy_base_asset.assetId = base_asset.assetId
        copy_base_asset.bestekPostNummer = base_asset.bestekPostNummer
        copy_base_asset.bestekPostNummer.remove(template_key)
        base_asset_toestand = base_asset.toestand
        created_assets = [copy_base_asset]

        # change the local id of the base asset to the real id in the mapping
        # and change relation id's accordingly
        base_local_id = next(local_id for local_id, asset_template in mapping.items() if asset_template['isHoofdAsset'])
        for local_id, asset_template in mapping.items():
            if local_id == base_local_id:
                continue
            if 'bronAssetId.identificator' in asset_template['attributen']:
                if asset_template['attributen']['bronAssetId.identificator']['value'] == base_local_id:
                    asset_template['attributen']['bronAssetId.identificator'][
                        'value'] = base_asset.assetId.identificator
                else:
                    asset_template['attributen']['bronAssetId.identificator']['value'] = \
                        f"{asset_template['attributen']['bronAssetId.identificator']['value']}_{asset_index}"

            if 'doelAssetId.identificator' in asset_template['attributen']:
                if asset_template['attributen']['doelAssetId.identificator']['value'] == base_local_id:
                    asset_template['attributen']['doelAssetId.identificator'][
                        'value'] = base_asset.assetId.identificator
                else:
                    asset_template['attributen']['doelAssetId.identificator']['value'] = \
                        f"{asset_template['attributen']['doelAssetId.identificator']['value']}_{asset_index}"

        for asset_to_create in mapping.keys():
            if asset_to_create != base_local_id:
                type_uri = mapping[asset_to_create]['typeURI']
                asset = dynamic_create_instance_from_uri(class_uri=type_uri)
                asset.assetId.identificator = f'{asset_to_create}_{asset_index}'
                created_assets.append(asset)
                if hasattr(asset, 'toestand'):
                    asset.toestand = base_asset_toestand
            else:
                asset = copy_base_asset

            for attr in mapping[asset_to_create]['attributen'].values():
                if attr['dotnotation'] == 'typeURI':
                    continue
                if attr['value'] is not None:
                    value = attr['value']
                    if attr['type'] == 'http://www.w3.org/2001/XMLSchema#decimal':
                        value = float(attr['value'])

                    if asset == copy_base_asset:
                        asset_attr = DotnotationHelper.get_attributes_by_dotnotation(
                            base_asset, dotnotation=attr['dotnotation'], waarde_shortcut_applicable=True)
                        if isinstance(asset_attr, list):
                            asset_attr = asset_attr[0]
                        if asset_attr.waarde is not None:
                            continue

                    DotnotationHelper.set_attribute_by_dotnotation(asset, dotnotation=attr['dotnotation'],
                                                                   waarde_shortcut_applicable=True, value=value)

        return created_assets

    @staticmethod
    def create_sqlite_if_not_exists(sqlite_path: Path):
        # create sqlite if not exists
        if not sqlite_path.exists():
            conn = sqlite3.connect(sqlite_path)
            c = conn.cursor()

            for q in ['''
CREATE TABLE state
(name text,
value text);
''', '''
CREATE TABLE contexts
(id text PRIMARY KEY,
starting_page text,
last_event_id text,
last_processed_event text);
''', '''
CREATE UNIQUE INDEX contexts_id_uindex ON contexts (id);
''', '''
CREATE TABLE contexts_assets
(context_id text,
asset_uuid text,
FOREIGN KEY (context_id) REFERENCES contexts (id));
            ''','''
CREATE TABLE aanleveringen
(event_id text PRIMARY KEY,
aanlevering_id text,
state text);
''']:
                c.execute(q)
            conn.commit()
            conn.close()

    def _save_to_sqlite_aanleveringen(self, event_id: str, aanlevering_id: str = None, state: str = None):
        conn = sqlite3.connect(self.sqlite_path)
        c = conn.cursor()
        c.execute('''SELECT event_id FROM aanleveringen WHERE event_id = ?''', (event_id,))
        if c.fetchone() is None:
            c.execute('''INSERT INTO aanleveringen VALUES (?, ?, ?)''', (event_id, aanlevering_id, state))
            conn.commit()
            conn.close()
            return
        if aanlevering_id is not None:
            c.execute('''UPDATE aanleveringen SET aanlevering_id = ? WHERE event_id = ?''', (aanlevering_id, event_id))
        if state is not None:
            c.execute('''UPDATE aanleveringen SET state = ? WHERE event_id = ?''', (state, event_id))
        conn.commit()
        conn.close()

    def get_aanlevering_by_event_id(self, event_id: str) -> tuple:
        conn = sqlite3.connect(self.sqlite_path)
        c = conn.cursor()

        c.execute('''SELECT event_id, aanlevering_id, state FROM aanleveringen WHERE event_id = ?''', (event_id,))
        row = c.fetchone()
        conn.commit()
        conn.close()
        return row

    def _save_to_sqlite_contexts(self, context_id: str, starting_page: str = None,
                                 last_event_id: str = None, last_processed_event: str = None):
        conn = sqlite3.connect(self.sqlite_path)
        c = conn.cursor()
        c.execute('''SELECT id FROM contexts WHERE id = ?''', (context_id,))
        if c.fetchone() is None:
            c.execute('''INSERT INTO contexts VALUES (?, ?, ?, ?)''', (context_id, starting_page, last_event_id,
                                                                       last_processed_event))
            conn.commit()
            conn.close()
            return
        if starting_page is not None:
            c.execute('''UPDATE contexts SET starting_page = ? WHERE id = ?''', (starting_page, context_id))
        if last_event_id is not None:
            c.execute('''UPDATE contexts SET last_event_id = ? WHERE id = ?''', (last_event_id, context_id))
        if last_processed_event is not None:
            c.execute('''UPDATE contexts SET last_processed_event = ? WHERE id = ?''',
                      (last_processed_event, context_id))
        conn.commit()
        conn.close()

    def _save_to_sqlite_contexts_assets(self, context_id: str, append: str):
        conn = sqlite3.connect(self.sqlite_path)
        c = conn.cursor()

        c.execute('''INSERT INTO contexts_assets VALUES (?, ?)''', (context_id, append))

        conn.commit()
        conn.close()

    def _get_asset_uuids_by_context_id(self, context_id: str):
        conn = sqlite3.connect(self.sqlite_path)
        c = conn.cursor()

        c.execute('''SELECT asset_uuid FROM contexts_assets WHERE context_id = ?''', (context_id,))
        results = [row[0] for row in c.fetchall()]
        conn.commit()
        conn.close()
        return results

    def get_last_processed_by_context_id(self, context_id) -> datetime.datetime:
        date_str = self.get_context_by_context_id(context_id)[3]
        return datetime.datetime.strptime(date_str, self.dt_format)

    def get_context_by_context_id(self, context_id) -> tuple:
        conn = sqlite3.connect(self.sqlite_path)
        c = conn.cursor()

        c.execute('''SELECT id, starting_page, last_event_id, last_processed_event FROM contexts WHERE id = ?''',
                  (context_id,))
        row = c.fetchone()

        conn.commit()
        conn.close()
        return row

    def check_if_already_done(self, context_uuid, entry_id: int) -> bool:
        conn = sqlite3.connect(self.sqlite_path)
        c = conn.cursor()
        c.execute("""SELECT id, last_event_id FROM contexts WHERE id like ? || '%'""", (context_uuid,))
        rows = c.fetchall()
        conn.commit()
        conn.close()

        for row in rows:
            if int(row[1]) >= entry_id:
                return True
        return False
