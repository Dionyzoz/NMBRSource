from abc import ABC
import re
from typing import Any, Iterable, List, Mapping, Optional, Tuple, Union
from airbyte_cdk.models.airbyte_protocol import SyncMode
from airbyte_cdk.sources.streams.http import HttpStream
import xml.etree.ElementTree as ET
import requests
import itertools


# Basic full refresh stream
class NmbrsStream(HttpStream, ABC):
    def __init__(self, employee, name):
        url_base = "https://api.nmbrs.nl/soap/v3/"
        self.url_base = (
            url_base
            + ("CompanyService.asmx" if employee else "EmployeeService.asmx")
            + "/"
        )
        self.soap_action_url = self.url_base.rstrip(".asmx")
        self.api_call = self.path()
        self.soap_action = self.soap_action_url + self.api_call

        self.name = name # schema name

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        return None

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

        yield all_data

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

    def request_body_data(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Union[Mapping[str, Any], str]]:
        if stream_slice is None:
            return f"""
            <soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:emp="https://api.nmbrs.nl/soap/v3/EmployeeService">
                <soap:Header>
                    <emp:AuthHeaderWithDomain>
                        <emp:Username>{self.username}</emp:Username>
                        <emp:Token>{self.token}</emp:Token>
                        <emp:Domain>{self.domain}</emp:Domain>
                    </emp:AuthHeaderWithDomain>
                </soap:Header>
                <soap:Body>
                    <emp:{self.api_call}/>
                </soap:Body>
            </soap:Envelope>
            """

        body = (
            +""">
                    <emp:"""
        )

        for key, value in stream_slice.items():
            body += f"""{key}>{value}</emp:{key}>
                    <emp:"""

        body = body[:-8] + f"</emp:{self.api_call}"

        return f"""
         <soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:emp="https://api.nmbrs.nl/soap/v3/EmployeeService">
            <soap:Header>
                <emp:AuthHeaderWithDomain>
                    <emp:Username>{self.username}</emp:Username>
                    <emp:Token>{self.token}</emp:Token>
                    <emp:Domain>{self.domain}</emp:Domain>
                </emp:AuthHeaderWithDomain>
            </soap:Header>
            <soap:Body>
                <emp:{body}>
            </soap:Body>
        </soap:Envelope>
        """


class NmbrsSliceStream(NmbrsStream, ABC):
    def __init__(self, partitions: Tuple[Tuple[str, Tuple[str]]], **kwargs: any):
        super().__init__(**kwargs)
        self.generators = []

    def stream_slices(
        self,
        *,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        for combination in itertools.product(
            generator() for generator in self.generators
        ):
            yield combination


class NmbrsSubStream(NmbrsStream, ABC):
    def __init__(self, parents: Tuple[Tuple[str, NmbrsStream]], **kwargs: Any):
        super().__init__(**kwargs)
        self.parents = parents
        self.generators.append(self.id_generators())

    def id_generators(self):
        generators = []
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
                        yield {idName: record["Id"]}

            generators.append(generator)


class Customers(NmbrsStream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "customer_id"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "customers"
