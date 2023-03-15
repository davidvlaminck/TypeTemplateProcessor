from unittest.mock import MagicMock

from EMInfraDomain import KenmerkEigenschapValueDTOList, KenmerkEigenschapValueDTO, EigenschapDTO

fake_attribute_list = KenmerkEigenschapValueDTOList(data=[KenmerkEigenschapValueDTO(eigenschap=EigenschapDTO(naam='eig1'))])
fake_attribute_list2 = KenmerkEigenschapValueDTOList(data=[KenmerkEigenschapValueDTO(eigenschap=EigenschapDTO(naam='eig2'))])


def return_fake_attribute_list(*args, **kwargs):
    if kwargs['ns'] == 'onderdeel' and kwargs['uuid'] == '00000000-0000-0000-0000-000000000001':
        return fake_attribute_list
    elif kwargs['ns'] == 'installatie' and kwargs['uuid'] == '00000000-0000-0000-0000-000000000002':
        return fake_attribute_list2
    else:
        return Exception("exception occurred")


m = MagicMock(side_effect=return_fake_attribute_list)

# {
#   "data": [
#     {
#       "typedValue": {
#         "_type": "number",
#         "value": 30
#       },
#       "determinedOn": "2023-03-14T14:52:18.538+01:00",
#       "determinedBy": "948a36d4-88ec-4a72-8f09-497a73d4983f",
#       "eigenschap": {
#         "uuid": "a4558509-25ef-4a2b-8726-c63519faa500",
#         "createdOn": "2021-04-29T10:11:00.413+02:00",
#         "modifiedOn": "2021-04-29T10:11:00.413+02:00",
#         "uri": "https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMObject.theoretischeLevensduur",
#         "naam": "theoretischeLevensduur",
#         "label": "theoretische levensduur",
#         "actief": true,
#         "alleenLezen": false,
#         "definitie": " De levensduur in aantal maanden die theoretisch mag verwacht worden voor een object.",
#         "categorie": "INVENTARISATIE",
#         "type": {
#           "_type": "datatype",
#           "datatype": {
#             "uuid": "30377b10-7e4c-41bd-9459-271ec7fb6764",
#             "createdOn": "2021-04-29T10:10:58.130+02:00",
#             "modifiedOn": "2021-04-29T10:10:58.130+02:00",
#             "naam": "KwantWrdInMaand",
#             "type": {
#               "_type": "number",
#               "eenheid": "mo"
#             },
#             "definitie": "Een kwantitatieve waarde die een getal in aantal maanden uitdrukt.",
#             "label": "Kwantitatieve waarde in maand",
#             "uri": "https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#KwantWrdInMaand",
#             "actief": true,
#             "links": [
#               {
#                 "rel": "self",
#                 "href": "https://apps-tei.mow.vlaanderen.be/eminfra/core/api/datatypes/30377b10-7e4c-41bd-9459-271ec7fb6764"
#               },
#               {
#                 "rel": "created-by",
#                 "href": "https://apps-tei.mow.vlaanderen.be/eminfra/identiteit/api/identiteiten/cbb6bd3b-33af-4bba-acdd-84ba0dfdf1e3"
#               },
#               {
#                 "rel": "modified-by",
#                 "href": "https://apps-tei.mow.vlaanderen.be/eminfra/identiteit/api/identiteiten/cbb6bd3b-33af-4bba-acdd-84ba0dfdf1e3"
#               }
#             ]
#           }
#         },
#         "kardinaliteitMin": 1,
#         "kardinaliteitMax": 1,
#         "links": [
#           {
#             "rel": "self",
#             "href": "https://apps-tei.mow.vlaanderen.be/eminfra/core/api/eigenschappen/a4558509-25ef-4a2b-8726-c63519faa500"
#           },
#           {
#             "rel": "created-by",
#             "href": "https://apps-tei.mow.vlaanderen.be/eminfra/identiteit/api/identiteiten/cbb6bd3b-33af-4bba-acdd-84ba0dfdf1e3"
#           },
#           {
#             "rel": "modified-by",
#             "href": "https://apps-tei.mow.vlaanderen.be/eminfra/identiteit/api/identiteiten/cbb6bd3b-33af-4bba-acdd-84ba0dfdf1e3"
#           }
#         ]
#       },
#       "actief": true,
#       "kenmerkType": {
#         "uuid": "f88f4fa1-bfdd-46ab-9908-9483931f59de",
#         "createdOn": "2022-06-13T14:27:10.846+02:00",
#         "modifiedOn": "2022-06-13T14:27:10.860+02:00",
#         "naam": "Eigenschappen - onderdeel#Beschermbuis",
#         "actief": true,
#         "predefined": false,
#         "standard": true,
#         "definitie": "Standaard kenmerk type voor onderdeel type 'onderdeel#Beschermbuis'",
#         "links": [
#           {
#             "rel": "self",
#             "href": "https://apps-tei.mow.vlaanderen.be/eminfra/core/api/kenmerktypes/f88f4fa1-bfdd-46ab-9908-9483931f59de"
#           },
#           {
#             "rel": "eigenschappen",
#             "href": "https://apps-tei.mow.vlaanderen.be/eminfra/core/api/kenmerktypes/f88f4fa1-bfdd-46ab-9908-9483931f59de/eigenschappen"
#           },
#           {
#             "rel": "created-by",
#             "href": "https://apps-tei.mow.vlaanderen.be/eminfra/identiteit/api/identiteiten/cbb6bd3b-33af-4bba-acdd-84ba0dfdf1e3"
#           },
#           {
#             "rel": "modified-by",
#             "href": "https://apps-tei.mow.vlaanderen.be/eminfra/identiteit/api/identiteiten/cbb6bd3b-33af-4bba-acdd-84ba0dfdf1e3"
#           }
#         ]
#       }
#     }
