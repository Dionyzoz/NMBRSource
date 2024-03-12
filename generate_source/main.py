import os
import re
from typing import List
import pandas as pd
import json
from queue import Queue

windows = "/mnt/c/Users/LucasJongsma"


def gen_schemas():
    nmbrs_meta = pd.read_excel(os.path.join(windows, "maturity.xlsx"))

    nmbrs_meta.columns = nmbrs_meta.iloc[0]
    nmbrs_meta: pd.DataFrame = nmbrs_meta.iloc[1:]

    tables = nmbrs_meta["Table"].unique()

    types = nmbrs_meta["Data-type"].unique()

    nmbrs_schema_folder = "/root/coding/nmbrs/source-nmbrs/src/source_nmbrs/schemas"

    type_mapping = {
        "int": "number",
        "float": "number",
        "bool": "boolean",
        "date": "string",
        "str": "string",
    }
    for table in tables:
        schema_dict = {"type": "object", "properties": {}}
        table_info = nmbrs_meta[nmbrs_meta["Table"] == table]
        schema_name = (
            f"{'_'.join([s.lower() for s in re.split('(?<=.)(?=[A-Z])', table)])}.json"
        )
        for index, row in table_info.iterrows():
            schema_dict["properties"][row["Fields"]] = {
                "type": ["null", type_mapping[row["Data-type"]]]
            }
            if row["Fields"] == "date":
                schema_dict["properties"][row["Fields"]]["format"] = "date"
                schema_dict["properties"][row["Fields"]][
                    "airbyte_type"
                ] = "timestamp_without_timezone"

        with open(os.path.join(nmbrs_schema_folder, schema_name), "w") as schema:
            schema.write(json.dumps(schema_dict, indent=2))


def underscored(t):
    return "_".join([s.lower() for s in re.split("(?<=.)(?=[A-Z])", t)])


def gen_sources():
    id_list_route = os.path.join(windows, "id_list")

    employee_calls = (
        pd.read_csv(os.path.join(windows, "API_Calls_Employee.csv"))
        .astype(str)
        .set_index("API_call")
    )
    company_calls = (
        pd.read_csv(os.path.join(windows, "API_Calls_Company.csv"))
        .astype(str)
        .set_index("API_call")
    )
    employee_deps = pd.read_csv(os.path.join(windows, "employee_deps.csv"))
    company_deps = (
        pd.read_csv(os.path.join(windows, "company_deps.csv"))
        .astype(str)
        .set_index("API_call")["IDs"]
    )
    employee_deps = employee_deps.set_index("API_call")["IDs"]

    nmbrs_meta = pd.read_excel(os.path.join(windows, "maturity.xlsx")).astype(str)

    nmbrs_meta.columns = nmbrs_meta.iloc[0]
    nmbrs_meta: pd.DataFrame = nmbrs_meta.iloc[1:]

    tables_call_map = (
        nmbrs_meta.astype(str).set_index("API-Call")["Table"].drop_duplicates()
    )

    table_employee_map = (
        nmbrs_meta.drop_duplicates(subset=["API-Call"])
        .astype(str)
        .set_index("API-Call")["Source"]
    )

    streams = []
    for api_call, table in tables_call_map.items():
        parents = []
        partitions = []
        employee = table_employee_map[api_call] == "Employee"
        if employee:
            row = employee_calls.loc[api_call]

        else:
            row = company_calls.loc[api_call]

        if row["API_type"] == "get":
            body_input = list(row["API_call_body_input"].split(";"))
            deps = list(row["API_dependencies"].split(";"))
            if body_input:
                body_input = list(filter(lambda x: x != "nan", body_input))

            if deps:
                deps = list(filter(lambda x: x != "nan", deps))

            for dep in deps:
                id = (employee_deps if employee else company_deps)[dep]
                parents.append((id, underscored(tables_call_map[dep])))
                body_input.remove(id)

            for _input in body_input:
                vals = list(
                    pd.read_csv(os.path.join(id_list_route, _input + ".csv"))
                    .set_index("index")
                    .iloc[:, 0]
                )
                partitions.append((_input, vals))

            classtype = (
                "NmbrsStream"
                if (not parents and not partitions)
                else "NmbrsSubStream"
                if parents
                else "NmbrsSliceStream"
            )

            def extract_tuple(parents):
                out = "("
                for id, parent in parents:
                    out += "("
                    out += f"'{id}',"
                    out += parent
                    out += "),"
                out += ")"

                return out

            streams.append(
                (
                    underscored(table),
                    set([parent[1] for parent in parents]),
                    f"{underscored(table)} = {classtype}(employee={employee}, name='{underscored(table)}', path='{api_call}' {',parents=' + extract_tuple(parents) if parents else ''} {',partitions=' + str(tuple(partitions)) if partitions else ''}, cache=_CACHE, authenticator=auth, config=config)\n",
                    f"{classtype}(employee={employee}, name='{underscored(table)}', path='{api_call}' {',parents=' + extract_tuple(parents) if parents else ''} {',partitions=' + str(tuple(partitions)) if partitions else ''}, authenticator=auth, config=config),\n",
                )
            )

    ordered_streams = []
    my_queue = Queue()

    new_streams = []
    for stream in streams:
        if not stream[1]:
            my_queue.put(stream[0])
            ordered_streams.append(stream)
        else:
            new_streams.append(stream)

    streams = new_streams
    has_dependents = set()
    while not my_queue.empty():
        name = my_queue.get()
        new_streams = []
        if streams:
            for stream in streams:
                if name in stream[1]:
                    has_dependents.add(name)
                    stream[1].discard(name)

                if not stream[1]:
                    my_queue.put(stream[0])
                    ordered_streams.append(stream)
                else:
                    new_streams.append(stream)
        else:
            break

        streams = new_streams

    with open("streams.py", "w") as file:
        for i in range(2):
            if i == 1:
                file.write("return [\n")
            for stream in ordered_streams:
                if i == 0:
                    if stream[0] in has_dependents:
                        file.write(stream[2])
                else:
                    file.write(
                        "\t"
                        + (
                            stream[0] + ",\n"
                            if stream[0] in has_dependents
                            else stream[3]
                        )
                    )

            if i == 1:
                file.write("]")


gen_sources()
gen_schemas()
