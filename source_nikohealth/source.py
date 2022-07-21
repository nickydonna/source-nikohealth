#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
import urllib.parse
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import pendulum
import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.requests_native_auth.oauth import Oauth2Authenticator
from pendulum import DateTime
from requests.auth import AuthBase


class NikoAuthenticator(AuthBase):
    _url: str
    _client_id: str
    _client_secret: str
    _token_type: Optional[str]
    _access_token: Optional[str]
    _token_expiry_date: DateTime

    def __init__(self, url: str, client_id: str, client_secret: str):
        self._client_id = client_id
        self._client_secret = client_secret
        self._url = url
        self._token_expiry_date = pendulum.now().subtract(days=1)

    def _refresh_token(self) -> [str, str, int]:
        res = requests.post(
            f"{self._url}",
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            auth=(self._client_id, self._client_secret),
            data="scope=external&grant_type=client_credentials"
        )
        json = res.json()
        return [json['token_type'], json['access_token'], json['expires_in']]

    def get_access_token(self) -> [str, str]:
        if self._token_has_expired():
            t0 = pendulum.now()
            token_type, token, expires_in = self._refresh_token()
            self._token_type = token_type
            self._access_token = token
            self._token_expiry_date = t0.add(seconds=expires_in)

        return [self._token_type, self._access_token]

    def _token_has_expired(self) -> bool:
        return pendulum.now() > self._token_expiry_date

    def __call__(self, r: requests.Request) -> requests.Request:
        token_type, access_token = self.get_access_token()
        r.headers["Authorization"] = f"{token_type} {access_token}"
        return r


class NikohealthStream(HttpStream, ABC):
    url_base = "https://better.nikohealth.com/api/external/"
    page_size = 100
    include_sensitive_data = False

    def __init__(self, domain: str, client_id: str, client_secret: str, **kwargs):
        super().__init__(**kwargs)
        self.domain = domain
        self.client_id = client_id
        self.client_secret = client_secret
        self.url = f"https://{domain}.nikohealth.com/api/external/"

    def get_json_schema(self) -> Mapping[str, Any]:
        schema = super().get_json_schema()
        if self.include_sensitive_data:
            return schema

        schema["properties"] = {
            key: key_schema
            for key, key_schema in schema["properties"].items()
            if "sensitive" not in key_schema or key_schema["sensitive"] is False
        }

        return schema

    def request_headers(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ):
        return {"Content-Type": "application/json", "Accept": "application/json"}

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        decoded_response = response.json()
        previous_query: dict = urllib.parse.parse_qs(
            urllib.parse.urlparse(response.request.url).query
        )
        previous_page = int(previous_query.get("pageIndex", ["0"])[0])
        previous_count = decoded_response.get("Count", 0)

        if previous_count > 0 and (previous_page * self.page_size) < previous_count:
            return {"pageIndex": previous_page + 1}

        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {"pageSize": self.page_size}

        if next_page_token:
            params.update(next_page_token)

        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data = response.json()['Items']
        if isinstance(data, list):
            return data
        return [data]


class Patients(NikohealthStream):
    primary_key = "Id"

    @property
    def use_cache(self) -> bool:
        return True

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "v1/patients"


class PatientInsurances(NikohealthStream, HttpSubStream):
    primary_key = 'Id'
    data_field = "call_metrics"

    def path(
        self,
        *,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"v1/patients/{stream_slice['parent']['Id']}/insurances"


# Source
class SourceNikohealth(AbstractSource):
    _auth: Optional[AuthBase] = None

    def auth(self, domain: str, client_id: str, client_secret: str) -> AuthBase:
        if self._auth is None:
            self._auth = NikoAuthenticator(
                client_id=client_id,
                client_secret=client_secret,
                url=f"https://{domain}.nikohealth.com/api/identity/connect/token",
            )

        return self._auth

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        client_id = config["client_id"]
        client_secret = config["client_secret"]
        domain = config["domain"]
        try:
            auth = self.auth(client_id, client_secret, domain)
            res = requests.get(
                f"https://{domain}.nikohealth.com/api/external/v1/hcpcs?pageSize=10",
                auth=auth
            )
            res.raise_for_status()
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        client_id = config["client_id"]
        client_secret = config["client_secret"]
        domain = config["domain"]
        auth = self.auth(domain, client_id, client_secret)
        base_args = dict(
            authenticator=auth,
            client_id=client_id,
            client_secret=client_secret,
            domain=domain
        )

        patients = Patients(**base_args)
        return [patients, PatientInsurances(parent=patients, **base_args)]
