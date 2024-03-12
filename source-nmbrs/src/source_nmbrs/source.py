#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from typing import Any, List, Mapping, Tuple

from airbyte_cdk.sources import AbstractSource
from .abstractstreams import NmbrsStream, NmbrsSubStream

_CACHE = True


# Source
class SourceNmbrs(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        print(config)
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[NmbrsStream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        auth = None

        companies = NmbrsStream(
            employee=False,
            name="companies",
            path="List_GetAll",
            cache=_CACHE,
            authenticator=auth,
            config=config,
        )
        employee_type = NmbrsStream(
            employee=True,
            name="employee_type",
            path="EmployeeType_GetList",
            cache=_CACHE,
            authenticator=auth,
            config=config,
        )
        run_info = NmbrsSubStream(
            employee=False,
            name="run_info",
            path="Run_GetList",
            parents=(("CompanyId", companies),),
            partitions=(("Year", [2024, 2023, 2022, 2021, 2020]),),
            cache=_CACHE,
            authenticator=auth,
            config=config,
        )
        employee = NmbrsSubStream(
            employee=True,
            name="employee",
            path="List_GetByCompany",
            parents=(
                ("CompanyId", companies),
                ("EmployeeType", employee_type),
            ),
            cache=_CACHE,
            authenticator=auth,
            config=config,
        )
        return [
            companies,
            employee_type,
            NmbrsSubStream(
                employee=False,
                name="company_current_adresses",
                path="Address_GetCurrent",
                parents=(("CompanyId", companies),),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=False,
                name="company_current_bank_acccount",
                path="BankAccount_GetCurrent",
                parents=(("CompanyId", companies),),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=False,
                name="contact_person",
                path="ContactPerson_Get",
                parents=(("CompanyId", companies),),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=False,
                name="company_cost_centers",
                path="CostCenter_GetList",
                parents=(("CompanyId", companies),),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=False,
                name="company_cost_units",
                path="CostUnit_GetList",
                parents=(("CompanyId", companies),),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=False,
                name="company_default_employee_templates",
                path="DefaultEmployeeTemplates_GetByCompany",
                parents=(("CompanyId", companies),),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=False,
                name="company_hour_codes",
                path="HourModel_GetHourCodes",
                parents=(("CompanyId", companies),),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=False,
                name="labour_agreement",
                path="LabourAgreements_Get",
                parents=(("CompanyId", companies),),
                partitions=(
                    ("Period", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]),
                    ("Year", [2024, 2023, 2022, 2021, 2020]),
                ),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=False,
                name="payroll_workflow",
                path="PayrollWorkflow_Get",
                parents=(("CompanyId", companies),),
                partitions=(
                    ("Period", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]),
                    ("Year", [2024, 2023, 2022, 2021, 2020]),
                ),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=False,
                name="pension_info",
                path="PensionExport_GetList",
                parents=(("CompanyId", companies),),
                partitions=(("intYear", [2024, 2023, 2022, 2021, 2020]),),
                authenticator=auth,
                config=config,
            ),
            run_info,
            NmbrsSubStream(
                employee=False,
                name="run_requests_by_company_year",
                path="RunRequest_GetList",
                parents=(("CompanyId", companies),),
                partitions=(("Year", [2024, 2023, 2022, 2021, 2020]),),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=False,
                name="company_current_schedules",
                path="Schedule_GetCurrent",
                parents=(("CompanyId", companies),),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=False,
                name="company_current_wage_components_var",
                path="WageComponentVar_Get",
                parents=(("CompanyId", companies),),
                partitions=(
                    ("Period", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]),
                    ("Year", [2024, 2023, 2022, 2021, 2020]),
                ),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=False,
                name="company_wage_codes",
                path="WageModel_GetWageCodes",
                parents=(("CompanyId", companies),),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=False,
                name="company_wage_taxes_by_company_year",
                path="WageTax_GetList",
                parents=(("CompanyId", companies),),
                partitions=(("intYear", [2024, 2023, 2022, 2021, 2020]),),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=False,
                name="work_costs_by_company_year",
                path="WorkCost_GetList",
                parents=(("CompanyId", companies),),
                partitions=(("Year", [2024, 2023, 2022, 2021, 2020]),),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=False,
                name="company_current_wage_components_fix",
                path="WageComponentFixed_Get",
                parents=(("CompanyId", companies),),
                partitions=(
                    ("Period", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]),
                    ("Year", [2024, 2023, 2022, 2021, 2020]),
                ),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=True,
                name="employee_absences_v2_by_employee",
                path="Absence_GetAll_AllEmployeesByCompany",
                parents=(("CompanyId", companies),),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=True,
                name="employee_addresses",
                path="Address_GetAll_AllEmployeesByCompany",
                parents=(("CompanyId", companies),),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=True,
                name="employee_children",
                path="Children_GetAll_Employeesbycompany",
                parents=(("CompanyId", companies),),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=True,
                name="employee_contracts",
                path="Contract_GetAll_AllEmployeesByCompany",
                parents=(("CompanyId", companies),),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=True,
                name="employee_cost_centers",
                path="CostCenter_GetAllEmployeesByCompany",
                parents=(("CompanyId", companies),),
                partitions=(
                    ("Period", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]),
                    ("Year", [2024, 2023, 2022, 2021, 2020]),
                ),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=True,
                name="employee_departments",
                path="Department_GetAll_AllEmployeesByCompany",
                parents=(("CompanyID", companies),),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=True,
                name="employee_employments",
                path="Employment_GetAll_AllEmployeesByCompany",
                parents=(("CompanyId", companies),),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=True,
                name="employee_lease_cars_by_company_year_p",
                path="LeaseCar_GetAll_EmployeesByCompany",
                parents=(("CompanyId", companies),),
                partitions=(
                    ("Period", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]),
                    ("Year", [2024, 2023, 2022, 2021, 2020]),
                ),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=True,
                name="employee_partners",
                path="Partner_GetAll_AllEmployeesByCompany",
                parents=(("CompanyId", companies),),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=True,
                name="employee_performance_reviews",
                path="PerformanceReview_GetAll_AllEmployeesByCompany",
                parents=(("CompanyId", companies),),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=True,
                name="employee_personal_info_contract_sal",
                path="PersonalInfoContractSalaryAddress_GetAll_AllEmployeesByCompany",
                parents=(("CompanyId", companies),),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=True,
                name="employee_personal_informations",
                path="PersonalInfo_GetAll_AllEmployeesByCompany",
                parents=(("CompanyId", companies),),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=True,
                name="employee_s_v_w_item",
                path="SVW_GetAll_AllEmployeesByCompany",
                parents=(("CompanyId", companies),),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=True,
                name="employee_salaries",
                path="Salary_GetAll_AllEmployeesByCompany",
                parents=(("CompanyId", companies),),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=True,
                name="employee_schedules",
                path="Schedule_GetAll_AllEmployeesByCompany",
                parents=(("CompanyId", companies),),
                authenticator=auth,
                config=config,
            ),
            employee,
            NmbrsSubStream(
                employee=False,
                name="employee_per_run",
                path="Run_GetEmployeesByRunCompany",
                parents=(
                    ("CompanyId", companies),
                    ("RunId", run_info),
                ),
                partitions=(("Year", [2024, 2023, 2022, 2021, 2020]),),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=True,
                name="employee_leave",
                path="Leave_GetList_V2",
                parents=(("EmployeeId", employee),),
                partitions=(
                    ("Type", ["Type1", "Type2", "Type3", "Type4", "Undefined"]),
                    ("Soort", ["Withdrawal", "Store", "Expired"]),
                    ("Year", [2024, 2023, 2022, 2021, 2020]),
                ),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=True,
                name="employee_absence",
                path="Absence_GetList",
                parents=(("EmployeeId", employee),),
                authenticator=auth,
                config=config,
            ),
            NmbrsSubStream(
                employee=True,
                name="leave_balance",
                path="LeaveBalance_Get",
                parents=(("EmployeeId", employee),),
                authenticator=auth,
                config=config,
            ),
        ]
