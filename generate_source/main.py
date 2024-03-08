import os
import re
from typing import List
import pandas as pd
import json

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

        with open(os.path.join(nmbrs_schema_folder, schema_name), "w") as schema:
            schema.write(json.dumps(schema_dict, indent=2))


def underscored(t):
    "_".join([s.lower() for s in re.split("(?<=.)(?=[A-Z])", t)])


def gen_sources():
    leave_absence_calls = pd.DataFrame.from_dict(
        {
            "API_call": (
                "Leave_GetList_V2",
                "Absence_GetList",
            ),
            "API_type": ("get", "get"),
            "API_call_body_input": ("EmployeeId;Type;Soort;Year", "EmployeeId"),
            "API_dependencies": ("List_GetByCompany", "List_GetByCompany"),
        },
    )
    id_list_route = os.path.join(windows, "id_list")

    employee_calls = pd.read_csv(os.path.join(windows, "API_Calls_Employee.csv"))
    company_calls = pd.read_csv(os.path.join(windows, "API_Calls_Company.csv"))
    combined_calls = pd.concat(
        [leave_absence_calls, employee_calls, company_calls], ignore_index=True
    ).set_index("API_call")
    employee_deps = pd.read_csv(os.path.join(windows, "employee_deps.csv"))
    leave_deps = pd.read_csv(os.path.join(windows, "leave_deps.csv"))
    company_deps = pd.read_csv(os.path.join(windows, "company_deps.csv"))

    dependency_table = (
        pd.concat([employee_deps, leave_deps, company_deps])
        .drop_duplicates(subset=["API_call"])
        .set_index("API_call")
        .stack()
    )

    nmbrs_meta = pd.read_excel(os.path.join(windows, "maturity.xlsx"))

    nmbrs_meta.columns = nmbrs_meta.iloc[0]
    nmbrs_meta: pd.DataFrame = nmbrs_meta.iloc[1:]

    tables_call_map = nmbrs_meta.set_index("API-Call")["Table"].drop_duplicates()
    tables_call_map = pd.concat(
        [
            tables_call_map,
            pd.Series(["LeaveBalance"], name="Table", index=["LeaveBalance_Get"]),
        ]
    )
    table_employee_map = nmbrs_meta.drop_duplicates(subset=["API-Call"]).set_index(
        "API-Call"
    )["Source"]

    table_employee_map = pd.concat(
        [
            table_employee_map,
            pd.Series(["Employee"], name="Table", index=["LeaveBalance_Get"]),
        ]
    )
    print(combined_calls)

    with open("streams.py", "w") as file:
        for api_call, table in tables_call_map.items():
            row = combined_calls[api_call]
            parents = []
            partitions = []
            if row["API_type"] == "get":
                body_input = row["API_call_body_input"].split(";")
                deps = row["API_dependencies"].split(";")
                for dep in deps:
                    id = dependency_table[dep]
                    parents.append((id, underscored(table)))
                    body_input.remove(id)

                for input in body_input:
                    vals = list(
                        pd.read_csv(os.path.join(id_list_route, input))
                        .set_index("index")
                        .stack()
                    )
                    partitions.append((input, vals))

                classtype = (
                    "NmbrsStream"
                    if (not parents and not partitions)
                    else "NmbrsSubStream"
                    if parents
                    else "NmbrsSliceStream"
                )

                file.writeline(
                    f"{underscored(table)} = {classtype}(employee={table_employee_map[api_call]} == 'Employee', name={underscored(table)} {',parents=' + parents if parents else ''} {',partitions=' + partitions if partitions else ''})"
                )


gen_sources()
