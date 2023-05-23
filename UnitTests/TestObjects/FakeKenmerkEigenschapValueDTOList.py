from unittest.mock import MagicMock

from EMInfraDomain import KenmerkEigenschapValueDTOList, KenmerkEigenschapValueDTO, EigenschapDTO, \
    EigenschapTypeDTOType, EigenschapTypeDTO, DatatypeTypeDTO, KenmerkTypeDTO, EigenschapTypedValueDTO

fake_attribute_list = KenmerkEigenschapValueDTOList(
    data=[KenmerkEigenschapValueDTO(eigenschap=EigenschapDTO(naam='eig1'))])
fake_attribute_list2 = KenmerkEigenschapValueDTOList(
    data=[KenmerkEigenschapValueDTO(eigenschap=EigenschapDTO(naam='eig2'))])


def return_fake_attribute_list(*args, **kwargs):
    if kwargs['ns'] == 'onderdeel' and kwargs['uuid'] == '00000000-0000-0000-0000-000000000001':
        return fake_attribute_list
    elif kwargs['ns'] == 'installatie' and kwargs['uuid'] == '00000000-0000-0000-0000-000000000002':
        return fake_attribute_list2
    else:
        raise Exception("exception occurred")


eig1 = KenmerkEigenschapValueDTO(
    eigenschap=EigenschapDTO(
        uuid='eig_bestekPostNummer',
        uri='https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMObject.bestekPostNummer',
        type=EigenschapTypeDTOType(datatype=EigenschapTypeDTO(type=DatatypeTypeDTO(_type='list')))),
    kenmerkType=KenmerkTypeDTO(uuid='kenmerktype_uuid'),
    typedValue=EigenschapTypedValueDTO(_type='text', value='valid_template_key'))
eig2 = KenmerkEigenschapValueDTO(
    eigenschap=EigenschapDTO(
        uuid='eig_typeURI',
        uri='https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMObject.typeURI'))
eig3 = KenmerkEigenschapValueDTO(
    eigenschap=EigenschapDTO(
        uuid='eig_isActief',
        uri='https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMDBStatus.isActief'))
eig4 = KenmerkEigenschapValueDTO(
    eigenschap=EigenschapDTO(
        uuid='eig_theoretischeLevensduur',
        uri='https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMObject.theoretischeLevensduur')
)
eig5 = KenmerkEigenschapValueDTO(
    eigenschap=EigenschapDTO(
        uuid='eig_bestekPostNummer',
        uri='https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMObject.bestekPostNummer',
        type=EigenschapTypeDTOType(datatype=EigenschapTypeDTO(type=DatatypeTypeDTO(_type='list')))),
    kenmerkType=KenmerkTypeDTO(uuid='kenmerktype_uuid'),
    typedValue=EigenschapTypedValueDTO(_type='list', value=[
        {'_type': 'text', 'value': 'valid_template_key'},
        {'_type': 'text', 'value': 'valid_template_key_2'}]))
eig6 = KenmerkEigenschapValueDTO(
    eigenschap=EigenschapDTO(
        uuid='eig_boolean_type',
        uri='https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMObject.boolean_type'))
eig7 = KenmerkEigenschapValueDTO(
    eigenschap=EigenschapDTO(
        uuid='eig_bestekPostNummer',
        uri='https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMObject.bestekPostNummer',
        type=EigenschapTypeDTOType(datatype=EigenschapTypeDTO(type=DatatypeTypeDTO(_type='list')))),
    kenmerkType=KenmerkTypeDTO(uuid='kenmerktype_uuid'),
    typedValue=EigenschapTypedValueDTO(_type='text', value='invalid_template_key'))

fake_full_attribute_list = KenmerkEigenschapValueDTOList(data=[eig1, eig2, eig3, eig4])
fake_full_attribute_list_two_template_keys = KenmerkEigenschapValueDTOList(data=[eig5])
fake_full_attribute_list_only_bestekpostnummer = KenmerkEigenschapValueDTOList(data=[eig1])
fake_full_attribute_list_without_template_key = KenmerkEigenschapValueDTOList(data=[])
fake_full_attribute_list_without_valid_template_key = KenmerkEigenschapValueDTOList(data=[eig7])
fake_full_attribute_list_with_one_valid_template_key_in_list = KenmerkEigenschapValueDTOList(data=[eig5])
