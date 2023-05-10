import datetime
import datetime
import pathlib
from pathlib import Path
from unittest.mock import Mock

from otlmow_davie.Enums import Environment, AuthenticationType
from otlmow_model.Classes.Onderdeel.Bevestiging import Bevestiging
from otlmow_model.Classes.Onderdeel.LEDDriver import LEDDriver
from otlmow_model.Classes.Onderdeel.VerlichtingstoestelLED import VerlichtingstoestelLED
from otlmow_model.Classes.Onderdeel.WVLichtmast import WVLichtmast

from EMInfraDomain import EntryObject, ContentObject, AtomValueObject, AggregateIdObject
from EMInfraRestClient import EMInfraRestClient
from TypeTemplateToAssetProcessor import TypeTemplateToAssetProcessor

THIS_FOLDER = pathlib.Path(__file__).parent


def create_processor_unittest_sqlite(sqlite_name: str) -> (EMInfraRestClient, TypeTemplateToAssetProcessor):
    sqlite_path = Path(THIS_FOLDER / sqlite_name)
    try:
        Path.unlink(Path(THIS_FOLDER / f'{sqlite_name}'))
    except FileNotFoundError:
        pass
    rest_client = Mock(spec=EMInfraRestClient)
    TypeTemplateToAssetProcessor._create_rest_client_based_on_settings = Mock()
    TypeTemplateToAssetProcessor._create_davie_client_based_on_settings = Mock()

    processor = TypeTemplateToAssetProcessor(sqlite_path=sqlite_path, settings_path=Path(),
                                             auth_type=AuthenticationType.JWT, environment=Environment.tei,
                                             postenmapping_path=Path('Postenmapping beschermbuis.state_db'))
    processor.rest_client = rest_client
    return rest_client, processor


def test_create_assets_by_template_correct():
    # TODO write test for wrong base asset
    _, processor = create_processor_unittest_sqlite('used_sqlite.db')

    postenmapping_dict = {
        "version": "0.3",
        "WVlichtmast_config1": {
            "Lichtmast1": {
                "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast",
                "attributen": {
                    "dwarsdoorsnede": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Lichtmast.dwarsdoorsnede",
                        "dotnotation": "dwarsdoorsnede",
                        "type": "None",
                        "value": "octagonaal",
                        "range": None
                    },
                    "masthoogte.standaardHoogte": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DtuLichtmastMasthoogte.standaardHoogte",
                        "dotnotation": "masthoogte.standaardHoogte",
                        "type": "None",
                        "value": "10.00",
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
                    "kleurArmatuur": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED.kleurArmatuur",
                        "dotnotation": "kleurArmatuur",
                        "type": "None",
                        "value": "ral-7038",
                        "range": None
                    },
                    "theoretischeLevensduur": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#KwantWrdInMaand.waarde",
                        "dotnotation": "theoretischeLevensduur.waarde",
                        "type": "None",
                        "value": "360",
                        "range": None
                    },
                },
                "isHoofdAsset": False
            },
            "LEDdriver1": {
                "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#LEDDriver",
                "attributen": {
                    "typeURI": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMObject.typeURI",
                        "dotnotation": "typeURI",
                        "type": "None",
                        "value": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#LEDDriver",
                        "range": None
                    }
                },
                "isHoofdAsset": False
            },
            "Bevestiging2": {
                "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging",
                "attributen": {
                    "bronAssetId.identificator": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#DtcIdentificator.identificator",
                        "dotnotation": "bronAssetId.identificator",
                        "type": "None",
                        "value": "LEDdriver1",
                        "range": None
                    },
                    "doelAssetId.identificator": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#DtcIdentificator.identificator",
                        "dotnotation": "doelAssetId.identificator",
                        "type": "None",
                        "value": "Lichtmast1",
                        "range": None
                    }
                },
                "isHoofdAsset": False
            },
        }
    }
    base_asset = WVLichtmast()
    base_asset.assetId.identificator = '0001'
    base_asset.toestand = 'in-gebruik'
    base_asset.bestekPostNummer = ['WVlichtmast_config1']
    base_asset.masthoogte.standaardHoogte = '12.50'

    processor.postenmapping_dict = postenmapping_dict
    result_list = processor.create_assets_from_template(template_key='WVlichtmast_config1', base_asset=base_asset, asset_index=0)

    assert len(result_list) == 5
    created_lichtmast = next(a for a in result_list if isinstance(a, WVLichtmast))
    created_led_toestel = next(a for a in result_list if isinstance(a, VerlichtingstoestelLED))
    created_led_driver = next(a for a in result_list if isinstance(a, LEDDriver))
    created_bevestiging1 = next(
        a for a in result_list if isinstance(a, Bevestiging) and a.assetId.identificator == 'Bevestiging1_0')
    created_bevestiging2 = next(
        a for a in result_list if isinstance(a, Bevestiging) and a.assetId.identificator == 'Bevestiging2_0')

    # check created are not None
    for asset in [created_bevestiging1, created_lichtmast, created_led_toestel, created_led_driver, created_bevestiging2]:
        assert asset is not None

    # check identificator
    assert created_bevestiging1.assetId.identificator == 'Bevestiging1_0'
    assert created_lichtmast.assetId.identificator == '0001'
    assert created_led_toestel.assetId.identificator == 'VerlichtingstoestelLED1_0'
    assert created_bevestiging2.assetId.identificator == 'Bevestiging2_0'
    assert created_led_driver.assetId.identificator == 'LEDdriver1_0'

    # check relations
    assert created_bevestiging1.bronAssetId.identificator == created_lichtmast.assetId.identificator
    assert created_bevestiging1.doelAssetId.identificator == created_led_toestel.assetId.identificator

    assert created_bevestiging2.bronAssetId.identificator == created_led_driver.assetId.identificator
    assert created_bevestiging2.doelAssetId.identificator == created_lichtmast.assetId.identificator

    # check other attributes
    assert created_lichtmast.bestekPostNummer == []
    assert created_lichtmast.dwarsdoorsnede == 'octagonaal'
    assert created_lichtmast.masthoogte.standaardHoogte is None
    assert created_led_toestel.toestand == 'in-gebruik'
    assert created_led_toestel.kleurArmatuur == 'ral-7038'
    assert created_led_toestel.theoretischeLevensduur.waarde == 360
