import pathlib

import pytest
from otlmow_model.Classes.Onderdeel.Beschermbuis import Beschermbuis
from otlmow_model.Classes.Onderdeel.LEDDriver import LEDDriver
from otlmow_model.Classes.Onderdeel.VoedtAangestuurd import VoedtAangestuurd

from Exceptions.AssetIdsNotUniqueError import AssetIdsNotUniqueError
from Exceptions.ValidBaseAssetMissingError import ValidBaseAssetMissingError
from Exceptions.TemplateKeyMissingError import TemplateKeyMissingError
from TypeTemplateCreator import TypeTemplateCreator

THIS_FOLDER = pathlib.Path(__file__).parent


def test_create_template_from_assets_success_simple_attribute():
    buis = Beschermbuis()
    buis.materiaal = 'hdpe'
    buis.bestekPostNummer = ['hdpe_bl_50_30']
    buis.assetId.identificator = 'Beschermbuis1'
    assets = [buis]

    template = TypeTemplateCreator.create_template_from_assets(assets=assets, base_asset_id='Beschermbuis1')

    expected_template = {
        "hdpe_bl_50_30": {
            "Beschermbuis1": {
                "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Beschermbuis",
                "attributen": {
                    "materiaal": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Beschermbuis.materiaal",
                        "dotnotation": "materiaal",
                        "type": "None",
                        "value": "hdpe",
                        "range": None
                    }
                },
                "isHoofdAsset": True
            }
        }
    }

    assert template == expected_template


def test_create_template_from_assets_success_kwant_wrd_attribute():
    buis = Beschermbuis()
    buis.theoretischeLevensduur.waarde = 360
    buis.bestekPostNummer = ['hdpe_bl_50_30']
    buis.assetId.identificator = 'Beschermbuis1'
    assets = [buis]

    template = TypeTemplateCreator.create_template_from_assets(assets=assets, base_asset_id='Beschermbuis1')

    expected_template = {
        "hdpe_bl_50_30": {
            "Beschermbuis1": {
                "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Beschermbuis",
                "attributen": {
                    "theoretischeLevensduur": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMObject.theoretischeLevensduur",
                        "dotnotation": "theoretischeLevensduur",
                        "type": "None",
                        "value": "360",
                        "range": None
                    }
                },
                "isHoofdAsset": True
            }
        }
    }

    assert template == expected_template


def test_create_template_from_assets_success_complex_attribute():
    relatie = VoedtAangestuurd()
    relatie.bronAssetId.identificator = 'Armatuurcontroller1'
    relatie.doelAssetId.identificator = 'LEDdriver1'
    relatie.bestekPostNummer = ['template']
    relatie.assetId.identificator = 'VoedtAangestuurd1'
    assets = [relatie]

    template = TypeTemplateCreator.create_template_from_assets(assets=assets, base_asset_id='VoedtAangestuurd1')

    expected_template = {
        "template": {
            "VoedtAangestuurd1": {
                "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VoedtAangestuurd",
                "attributen": {
                    "bronAssetId.identificator": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#DtcIdentificator.identificator",
                        "dotnotation": "bronAssetId.identificator",
                        "type": "None",
                        "value": "Armatuurcontroller1",
                        "range": None
                    },
                    "doelAssetId.identificator": {
                        "typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#DtcIdentificator.identificator",
                        "dotnotation": "doelAssetId.identificator",
                        "type": "None",
                        "value": "LEDdriver1",
                        "range": None
                    }
                },
                "isHoofdAsset": True
            }
        }
    }

    assert template == expected_template


def test_create_template_from_assets_success_no_attributes():
    leddriver = LEDDriver()
    leddriver.bestekPostNummer = ['template']
    leddriver.assetId.identificator = 'LEDdriver1'
    assets = [leddriver]

    template = TypeTemplateCreator.create_template_from_assets(assets=assets, base_asset_id='LEDdriver1')

    expected_template = {
        "template": {
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
                "isHoofdAsset": True
            },
        }
    }

    assert template == expected_template


def test_create_template_from_assets_failure_no_bestekpostnummer_in_base_asset():
    buis = Beschermbuis()
    buis.materiaal = 'hdpe'
    buis.assetId.identificator = 'Beschermbuis1'
    assets = [buis]

    with pytest.raises(TemplateKeyMissingError) as exc:
        TypeTemplateCreator.create_template_from_assets(assets=assets, base_asset_id='Beschermbuis1')

    assert str(exc.value) == 'Base asset has no value in bestekPostNummer (used as template key)'


def test_create_template_from_assets_failure_base_asset_not_found():
    buis = Beschermbuis()
    buis.materiaal = 'hdpe'
    buis.assetId.identificator = 'Beschermbuis1'
    assets = [buis]

    with pytest.raises(ValidBaseAssetMissingError) as exc:
        TypeTemplateCreator.create_template_from_assets(assets=assets, base_asset_id='id')

    assert str(exc.value) == "Base asset not found by id ('id')"


def test_create_template_from_assets_failure_ids_not_unique():
    buis = Beschermbuis()
    buis.assetId.identificator = 'Beschermbuis1'
    buis2 = Beschermbuis()
    buis2.assetId.identificator = 'Beschermbuis1'
    assets = [buis, buis2]

    with pytest.raises(AssetIdsNotUniqueError) as exc:
        TypeTemplateCreator.create_template_from_assets(assets=assets, base_asset_id='')

    assert str(exc.value) == "Not all assets have a unique id"
