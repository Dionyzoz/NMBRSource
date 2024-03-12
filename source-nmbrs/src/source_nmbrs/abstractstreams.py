import re
from typing import Any, Iterable, List, Mapping, Optional, Tuple, Union
from airbyte_cdk.models.airbyte_protocol import SyncMode
from airbyte_cdk.sources.streams.http import HttpStream
import xml.etree.ElementTree as ET
import requests
import itertools


# Example usage:

# Create a logger

# Basic full refresh stream
class NmbrsStream(HttpStream):
    primary_key = None

    url_base = "https://api.nmbrs.nl/soap/v3/"
    @property
    def name(self):
        return self._name

    @property
    def use_cache(self):
        return self._use_cache

    def __init__(self, employee, name, path, config, cache=False, **kwargs):
        self.username = config["username"]
        self.token = config["token"]
        self.domain = config["domain"]

        self.soap_action_url = self.url_base + ("CompanyService" if not employee else "EmployeeService")
        self.api_call = path
        self._path = "CompanyService.asmx" if not employee else "EmployeeService.asmx"
        self.body_tag = "com" if not employee else "emp"
        self.soap_action = self.soap_action_url + '/' + self.api_call

        self._name = name # schema name
        self._use_cache = cache
        super().__init__(**kwargs)

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        return None

    def path(self, *args, **kwargs) -> str:
        return self._path

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping]:
        xml_data = response.content
        if isinstance(xml_data, bytes):
            xml_data = xml_data.decode("utf-8")

        # Remove the namespaces from the XML for easier parsing
        xml_data = re.sub(' xmlns="[^"]+"', "", xml_data, count=1)

        # Parse the XML data
        root = ET.fromstring(xml_data).find(f".//{self.api_call}Result")

        # List to hold all rows of data
        all_data = []

        # Function to recursively extract data from elements
        def extract_data_recursive(element, base_data=None):
            if base_data is None:
                base_data = {}

            child_data = {}
            try:
                for child in element:
                    if len(child) > 0:
                        # Recursively extract data from nested elements
                        extract_data_recursive(
                            child, base_data={**base_data, **child_data}
                        )
                    else:
                        child_data[child.tag] = child.text

            except Exception:
                pass

            if child_data and not any(len(child) > 0 for child in element):
                all_data.append({**base_data, **child_data})

        extract_data_recursive(root)

        return all_data

    def request_headers(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        headers = {
            "SOAPAction": self.soap_action,
            "Content-Type": 'text/xml;charset="utf-8"',
            "Accept": "text/xml",
        }
        return headers
    
    @property
    def http_method(self, *args, **kwargs):
        return "POST"

    def request_body_data(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Union[Mapping[str, Any], str]]:
        if stream_slice is None:
            return f"""
            <soapenv:Envelope xmlns:soapenv="http://www.w3.org/2003/05/soap-envelope" xmlns:{self.body_tag}="{self.soap_action_url}">
                <soapenv:Header>
                    <{self.body_tag}:AuthHeaderWithDomain>
                        <{self.body_tag}:Username>{self.username}</{self.body_tag}:Username>
                        <{self.body_tag}:Token>{self.token}</{self.body_tag}:Token>
                        <{self.body_tag}:Domain>{self.domain}</{self.body_tag}:Domain>
                    </{self.body_tag}:AuthHeaderWithDomain>
                </soapenv:Header>
                <soapenv:Body>
                    <{self.body_tag}:{self.api_call}/>
                </soapenv:Body>
            </soapenv:Envelope>
            """

        body = (
            self.api_call + f""">
                    <{self.body_tag}:"""
        )

        for key, value in stream_slice.items():
            body += f"""{key}>{value}</{self.body_tag}:{key}>
                    <{self.body_tag}:"""

        body = body[:-8] + f"</{self.body_tag}:{self.api_call}"

        x = f"""
         <soapenv:Envelope xmlns:soapenv="http://www.w3.org/2003/05/soap-envelope" xmlns:{self.body_tag}="{self.soap_action_url}">
            <soapenv:Header>
                <{self.body_tag}:AuthHeaderWithDomain>
                    <{self.body_tag}:Username>{self.username}</{self.body_tag}:Username>
                    <{self.body_tag}:Token>{self.token}</{self.body_tag}:Token>
                    <{self.body_tag}:Domain>{self.domain}</{self.body_tag}:Domain>
                </{self.body_tag}:AuthHeaderWithDomain>
            </soapenv:Header>
            <soapenv:Body>
                <{self.body_tag}:{body}>
            </soapenv:Body>
        </soapenv:Envelope>
        """
        print(x)
        return x
    


class NmbrsSliceStream(NmbrsStream):
    def __init__(self, partitions: Tuple[Tuple[str, Tuple[str]]] = (), **kwargs: any):
        super().__init__(**kwargs)
        self.generators = []
        for partition in partitions:
            self.generators.append(lambda _partition=partition: ((_partition[0], part) for part in _partition[1]))

    def stream_slices(
        self,
        *,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        for combination in itertools.product(
            *[generator() for generator in self.generators]
        ):
            print(combination)
            yield dict(combination)


class NmbrsSubStream(NmbrsSliceStream):
    def __init__(self, parents: Tuple[Tuple[str, NmbrsStream]], **kwargs: Any):
        super().__init__(**kwargs)
        self.parents = parents
        self.id_generators()

    def id_generators(self):
        for idName, parentStream in self.parents:

            def generator():
                parentStream_stream_slices = parentStream.stream_slices(
                    sync_mode=SyncMode.full_refresh,
                )
                for stream_slice in parentStream_stream_slices:
                    parent_records = parentStream.read_records(
                        sync_mode=SyncMode.full_refresh, stream_slice=stream_slice
                    )

                    for record in parent_records:
                        yield (idName, record.get("Id", record["ID"]))

            self.generators.append(generator)


