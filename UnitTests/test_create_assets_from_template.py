import datetime
import datetime
import pathlib
from pathlib import Path
from unittest.mock import Mock

from otlmow_davie.Enums import Environment, AuthenticationType
from otlmow_model.Classes.Onderdeel.Bevestiging import Bevestiging
from otlmow_model.Classes.Onderdeel.VerlichtingstoestelLED import VerlichtingstoestelLED
from otlmow_model.Classes.Onderdeel.WVLichtmast import WVLichtmast

from EMInfraDomain import EntryObject, ContentObject, AtomValueObject, AggregateIdObject
from EMInfraRestClient import EMInfraRestClient
from TypeTemplateToAssetProcessor import TypeTemplateToAssetProcessor

THIS_FOLDER = pathlib.Path(__file__).parent


def create_processor_unittest_shelve(shelve_name: str) -> (EMInfraRestClient, TypeTemplateToAssetProcessor, Path):
    shelve_path = Path(THIS_FOLDER / shelve_name)
    try:
        Path.unlink(Path(THIS_FOLDER / f'{shelve_name}.db'))
    except FileNotFoundError:
        pass
    rest_client = Mock(spec=EMInfraRestClient)
    TypeTemplateToAssetProcessor._create_rest_client_based_on_settings = Mock()
    TypeTemplateToAssetProcessor._create_davie_client_based_on_settings = Mock()

    processor = TypeTemplateToAssetProcessor(shelve_path=shelve_path, settings_path=None, auth_type=AuthenticationType.JWT,
                                             environment=Environment.tei,
                                             postenmapping_path=Path('Postenmapping beschermbuis.db'))
    processor.rest_client = rest_client
    return rest_client, processor, shelve_path


def delete_unittest_shelve(shelve_name: str) -> None:
    import os
    prefixed = [filename for filename in os.listdir('.') if filename.startswith(shelve_name)]
    for filename in prefixed:
        try:
            Path.unlink(Path(THIS_FOLDER / filename))
        except FileNotFoundError:
            pass


def test_create_assets_by_template_correct():
    # TODO write test for wrong base asset
    shelve_name = 'db_unittests_17'
    _, processor, _ = create_processor_unittest_shelve(shelve_name=shelve_name)

    postenmapping_dict = {
        "version": "0.3",
        "WVlichtmast_config1": {
            "Lichtmast1": {
                "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast",
                "attributen": {
                    "beschermlaag": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Lichtmast.beschermlaag",
                        "dotnotation": "beschermlaag",
                        "type": "None",
                        "value": "gegalvaniseerd",
                        "range": None
                    },
                    "dwarsdoorsnede": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Lichtmast.dwarsdoorsnede",
                        "dotnotation": "dwarsdoorsnede",
                        "type": "None",
                        "value": "octagonaal",
                        "range": None
                    },
                    "heeftStopcontact": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Lichtmast.heeftStopcontact",
                        "dotnotation": "heeftStopcontact",
                        "type": "None",
                        "value": "False",
                        "range": None
                    },
                    "masthoogte.standaardHoogte": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DtuLichtmastMasthoogte.standaardHoogte",
                        "dotnotation": "masthoogte.standaardHoogte",
                        "type": "None",
                        "value": "10.00",
                        "range": None
                    },
                    "masttype": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Lichtmast.masttype",
                        "dotnotation": "masttype",
                        "type": "None",
                        "value": "RM",
                        "range": None
                    },
                    "normeringBotsvriendelijk": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Lichtmast.normeringBotsvriendelijk",
                        "dotnotation": "normeringBotsvriendelijk",
                        "type": "None",
                        "value": "niet-botsvriendelijke-mast",
                        "range": None
                    },
                    "aantalArmen": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast.aantalArmen",
                        "dotnotation": "aantalArmen",
                        "type": "None",
                        "value": "0",
                        "range": None
                    },
                    "armlengte": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast.armlengte",
                        "dotnotation": "armlengte",
                        "type": "None",
                        "value": "niet-van-toepassing",
                        "range": None
                    }
                },
                "isHoofdAsset": True
            },
            "Bevestiging1": {
                "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging",
                "attributen": {
                    "bronAssetId.identificator": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#DtcIdentificator.identificator",
                        "dotnotation": "bronAssetId.identificator",
                        "type": "None",
                        "value": "Lichtmast1",
                        "range": None
                    },
                    "doelAssetId.identificator": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#DtcIdentificator.identificator",
                        "dotnotation": "doelAssetId.identificator",
                        "type": "None",
                        "value": "VerlichtingstoestelLED1",
                        "range": None
                    }
                },
                "isHoofdAsset": False
            },
            "VerlichtingstoestelLED1": {
                "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED",
                "attributen": {
                    "lumenOutput": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED.lumenOutput",
                        "dotnotation": "lumenOutput",
                        "type": "None",
                        "value": "12500",
                        "range": None
                    },
                    "aantalTeVerlichtenRijstroken": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED.aantalTeVerlichtenRijstroken",
                        "dotnotation": "aantalTeVerlichtenRijstroken",
                        "type": "None",
                        "value": "2",
                        "range": None
                    },
                    "kleurArmatuur": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED.kleurArmatuur",
                        "dotnotation": "kleurArmatuur",
                        "type": "None",
                        "value": "ral-7038",
                        "range": None
                    },
                    "heeftAntiVandalisme": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED.heeftAntiVandalisme",
                        "dotnotation": "heeftAntiVandalisme",
                        "type": "None",
                        "value": "False",
                        "range": None
                    },
                    "isFaunavriendelijk": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED.isFaunavriendelijk",
                        "dotnotation": "isFaunavriendelijk",
                        "type": "None",
                        "value": "False",
                        "range": None
                    },
                    "isLijnvormig": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED.isLijnvormig",
                        "dotnotation": "isLijnvormig",
                        "type": "None",
                        "value": "False",
                        "range": None
                    },
                    "lichtpuntHoogte": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED.lichtpuntHoogte",
                        "dotnotation": "lichtpuntHoogte",
                        "type": "None",
                        "value": "10",
                        "range": None
                    },
                    "merk": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/abstracten#Verlichtingstoestel.merk",
                        "dotnotation": "merk",
                        "type": "None",
                        "value": "Schreder",
                        "range": None
                    },
                    "modelnaam": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/abstracten#Verlichtingstoestel.modelnaam",
                        "dotnotation": "modelnaam",
                        "type": "None",
                        "value": "izylum",
                        "range": None
                    }
                },
                "isHoofdAsset": False
            }
        }
    }
    base_asset = WVLichtmast()
    base_asset.assetId.identificator = '0001'
    base_asset.toestand = 'in-gebruik'
    base_asset.bestekPostNummer = ['WVlichtmast_config1']

    processor.postenmapping_dict = postenmapping_dict
    result_list = processor.create_assets_from_template(template_key='WVlichtmast_config1', base_asset=base_asset, asset_index=0)

    assert len(result_list) == 3
    created_bevestiging = next(a for a in result_list if isinstance(a, Bevestiging))
    created_lichtmast = next(a for a in result_list if isinstance(a, WVLichtmast))
    created_led_toestel = next(a for a in result_list if isinstance(a, VerlichtingstoestelLED))
    assert created_bevestiging is not None
    assert created_lichtmast is not None
    assert created_led_toestel is not None

    assert created_bevestiging.bronAssetId.identificator == created_lichtmast.assetId.identificator
    assert created_bevestiging.doelAssetId.identificator == created_led_toestel.assetId.identificator

    assert created_bevestiging.assetId.identificator == 'Bevestiging1_0'
    assert created_lichtmast.assetId.identificator == '0001'
    assert created_led_toestel.assetId.identificator == 'VerlichtingstoestelLED1_0'

    assert created_lichtmast.bestekPostNummer == []
    assert created_lichtmast.dwarsdoorsnede == 'octagonaal'

    assert created_led_toestel.toestand == 'in-gebruik'
    assert created_led_toestel.kleurArmatuur == 'ral-7038'

    delete_unittest_shelve(shelve_name)
