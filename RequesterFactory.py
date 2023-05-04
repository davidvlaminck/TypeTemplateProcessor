from typing import Dict, Union

from otlmow_davie.Enums import AuthenticationType, Environment

from CertRequester import CertRequester
from JWTRequester import JWTRequester


class RequesterFactory:
    @staticmethod
    def create_requester(settings: Dict, auth_type: AuthenticationType, environment: Environment
                         ) -> Union[CertRequester, JWTRequester]:
        auth_info = settings['authentication'][auth_type.name][environment.name]

        first_part_url = ''
        if environment == Environment.prd:
            first_part_url = 'https://services.apps.mow.vlaanderen.be/'
        elif environment == Environment.tei:
            first_part_url = 'https://services.apps-tei.mow.vlaanderen.be/'
        elif environment == Environment.dev:
            first_part_url = 'https://services.apps-dev.mow.vlaanderen.be/'

        if auth_type == AuthenticationType.JWT:
            return JWTRequester(private_key_path=auth_info['key_path'],
                                client_id=auth_info['client_id'],
                                first_part_url=first_part_url)
        if auth_type == AuthenticationType.cert:
            return CertRequester(cert_path=auth_info['cert_path'],
                                 key_path=auth_info['key_path'],
                                 first_part_url=first_part_url)

