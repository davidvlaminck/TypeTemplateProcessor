import copy

from EMInfraDomain import EigenschapDTO, EigenschapTypeDTOType, EigenschapTypeDTO, DatatypeTypeDTO

fake_eig_theoretische_levensduur = EigenschapDTO(
    uuid='eig_theoretischeLevensduur',
    uri='https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMObject.theoretischeLevensduur',
    type=EigenschapTypeDTOType(datatype=EigenschapTypeDTO(type=DatatypeTypeDTO(_type='number'))))
fake_eig_materiaal = EigenschapDTO(
    uuid='eig_materiaal',
    uri='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Beschermbuis.materiaal',
    type=EigenschapTypeDTOType(datatype=EigenschapTypeDTO(type=DatatypeTypeDTO(_type='keuzelijst'))))
fake_eig_buitendiameter = EigenschapDTO(
    uuid='eig_buitendiameter',
    uri='https://wegenenverkeer.data.vlaanderen.be/ns/abstracten#Leiding.buitendiameter',
    type=EigenschapTypeDTOType(datatype=EigenschapTypeDTO(type=DatatypeTypeDTO(_type='number'))))
fake_eig_not_implemented_datatype = EigenschapDTO(
    uuid='eig_not_implemented_type',
    uri='https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMObject.not_implemented_type',
    type=EigenschapTypeDTOType(datatype=EigenschapTypeDTO(type=DatatypeTypeDTO(_type='not_implemented_type'))))
fake_eig_boolean_type = EigenschapDTO(
    uuid='eig_boolean_type',
    uri='https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMObject.boolean_type',
    type=EigenschapTypeDTOType(datatype=EigenschapTypeDTO(type=DatatypeTypeDTO(_type='boolean'))))

fake_eig_dict = {
    'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMObject.theoretischeLevensduur':
        fake_eig_theoretische_levensduur,
    'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Beschermbuis.materiaal': fake_eig_materiaal,
    'https://wegenenverkeer.data.vlaanderen.be/ns/abstracten#Leiding.buitendiameter': fake_eig_buitendiameter,
    'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMObject.not_implemented_type':
        fake_eig_not_implemented_datatype,
    'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMObject.boolean_type': fake_eig_boolean_type
}


def return_fake_eigenschap(*args, **kwargs):
    return fake_eig_dict[kwargs['uri']]
