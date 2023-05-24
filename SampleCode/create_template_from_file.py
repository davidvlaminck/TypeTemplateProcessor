import json
import pathlib
from pathlib import Path

from otlmow_converter.OtlmowConverter import OtlmowConverter

from PostenMappingDict import PostenMappingDict
from TypeTemplateCreator import TypeTemplateCreator


# from otlmow_postenmapping
def _write_posten_mapping(posten_mapping: dict, directory: Path = None) -> None:
    posten_mapping_str = json.dumps(posten_mapping, indent=4)
    file_dir = directory
    if file_dir is None:
        file_dir = Path(__file__).parent
    file_path = file_dir / 'PostenMappingDict.py'
    with open(file_path, "w") as file:
        file.write('class PostenMappingDict:\n    mapping_dict = ' + posten_mapping_str.replace('null', 'None').
                   replace('true', 'True').replace('false', 'False'))


if __name__ == '__main__':
    this_directory = pathlib.Path(__file__).parent
    input_file_path = Path(this_directory / 'Lichtmast1.json')
    file_name = input_file_path.stem

    converter = OtlmowConverter()
    assets = converter.create_assets_from_file(filepath=input_file_path)

    new_template = TypeTemplateCreator.create_template_from_assets(assets=assets, base_asset_id=file_name)
    PostenMappingDict.mapping_dict.update(new_template)

    _write_posten_mapping( PostenMappingDict.mapping_dict, directory=this_directory)
