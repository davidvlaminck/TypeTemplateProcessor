from otlmow_model.BaseClasses.OTLObject import get_attribute_by_name

from Exceptions.AssetIdsNotUniqueError import AssetIdsNotUniqueError
from Exceptions.TemplateKeyMissingError import TemplateKeyMissingError
from Exceptions.ValidBaseAssetMissingError import ValidBaseAssetMissingError


class TypeTemplateCreator:
    @classmethod
    def create_template_from_assets(cls, assets: list, base_asset_id: str) -> dict:
        # perform validity checks
        unique_id_count = len(set(asset.assetId.identificator for asset in assets))
        if unique_id_count != len(assets):
            raise AssetIdsNotUniqueError('Not all assets have a unique id')
        base_asset_list = [asset for asset in assets if asset.assetId.identificator == base_asset_id]
        if len(base_asset_list) == 0:
            raise ValidBaseAssetMissingError(f"Base asset not found by id ('{base_asset_id}')")
        base_asset = [asset for asset in assets if asset.assetId.identificator == base_asset_id][0]
        if base_asset.bestekPostNummer is None or len(base_asset.bestekPostNummer) == 0:
            raise TemplateKeyMissingError('Base asset has no value in bestekPostNummer (used as template key)')

        template_key = base_asset.bestekPostNummer[0]
        template = {}
        for asset in assets:
            asset_template = {}
            d = asset.create_dict_from_asset()
            for attribute_name in d:
                if attribute_name == 'assetId' or attribute_name == 'bestekPostNummer' or attribute_name == 'typeURI':
                    continue
                cls.add_attribute_to_asset_template_dict(asset, asset_template, attribute_name)

            if len(asset_template) == 0:
                asset_template['typeURI'] = {
                    'dotnotation': 'typeURI',
                    'range': None,
                    'type': "None",
                    'typeURI': 'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMObject.typeURI',
                    'value': asset.typeURI}

            template[asset.assetId.identificator] = {
                'attributen': asset_template,
                'typeURI': asset.typeURI,
                'isHoofdAsset': asset.assetId.identificator == base_asset_id}

        return {template_key: template}

    @classmethod
    def add_attribute_to_asset_template_dict(cls, asset, asset_template, attribute_name, prefix: str = ''):
        attr = get_attribute_by_name(asset, attribute_name)
        value = attr.waarde
        if value is None:
            return
        if isinstance(value, list):
            raise NotImplementedError
        elif attr.field.waardeObject is not None:
            if attr.field.waarde_shortcut_applicable:
                waarde_attr = get_attribute_by_name(attr.waarde, 'waarde')
                if prefix == '':
                    dotnotation = attr.naam
                else:
                    dotnotation = prefix + '.' + attr.naam
                asset_template[dotnotation] = {
                    'dotnotation': dotnotation,
                    'range': None,
                    'type': "None",
                    'typeURI': attr.objectUri,
                    'value': str(waarde_attr.waarde)}
            else:
                for k in value:
                    cls.add_attribute_to_asset_template_dict(value, asset_template, k.naam, prefix=attribute_name)
        else:
            attr = get_attribute_by_name(asset, attribute_name)
            if prefix == '':
                dotnotation = attr.naam
            else:
                dotnotation = prefix + '.' + attr.naam
            asset_template[dotnotation] = {
                'dotnotation': dotnotation,
                'range': None,
                'type': "None",
                'typeURI': attr.objectUri,
                'value': attr.waarde}
