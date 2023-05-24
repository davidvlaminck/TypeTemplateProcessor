import pathlib
from pathlib import Path

from otlmow_converter.OtlmowConverter import OtlmowConverter
from TypeTemplateToAssetProcessor import TypeTemplateToAssetProcessor

if __name__ == '__main__':
    converter = OtlmowConverter()
    this_directory = pathlib.Path(__file__).parent
    processor = TypeTemplateToAssetProcessor(sqlite_path=Path(this_directory / 'local.state_db'), offline=True,
                                             auth_type=None, settings_path=None, environment=None)

    input_file_path = Path(this_directory / 'DA-2023-WVLichtmast_voorbeeld.xlsx')
    export_file_path = Path(this_directory / 'DA-2023-WVLichtmast_na_templates.xlsx')

    assets = converter.create_assets_from_file(filepath=input_file_path)
    lichtmasten = [asset for asset in assets  # behoud alleen lichtmasten
                   if asset.typeURI == 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast']

    for lichtmast in lichtmasten:  # pas template toe op alle masten in het bestand
        lichtmast.bestekPostNummer = ['WVlichtmast_config1']

    alle_assets = processor.process_objects_using_template(objects_to_process=lichtmasten)

    for lichtmast in [asset for asset in alle_assets
                      if asset.typeURI == 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast']:
        lichtmast.bestekPostNummer = None  # verwijder de template key manueel (= workaround bug)

    converter.create_file_from_assets(list_of_objects=alle_assets, filepath=export_file_path)
