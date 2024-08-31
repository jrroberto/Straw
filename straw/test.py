from pprint import pprint, PrettyPrinter
from typing import Callable

import numpy as np
import pandas as pd
from benedict import benedict

from straw.df_transform import DataFrameTransformer
from straw.reader import TabularDataReader
from straw.settings import DataSettings, FileSettings
from straw.settings import Default as DefaultSettings
from straw.mapping import ColumnMapping
source_identifier_starts_with = ColumnMapping.source_identifier_starts_with
source_identifier_ends_with = ColumnMapping.source_identifier_ends_with
source_identifier_contains = ColumnMapping.source_identifier_contains

TEST_DATA_DIRECTORY = 'data/test'


def run_test_case(func: Callable) -> bool:
	print(f"Running test case {func.__name__}")
	passed = False
	try:
		passed = func()
	except Exception as e:
		print(e)
	print(f"{func.__name__} {'PASSED' if passed else 'FAILED'}")
	print('')
	return passed


def test_read_csv() -> bool:
	reader = TabularDataReader()
	df = reader.read_csv(f"{TEST_DATA_DIRECTORY}/sample01.csv")
	print(df)
	df.info()
	return True


def test_read_csv_pipe_separated() -> bool:
	file_settings = FileSettings(separator='|')
	reader = TabularDataReader(file_settings=file_settings)
	df = reader.read_csv(f"{TEST_DATA_DIRECTORY}/sample02.csv")
	print(df)
	df.info()
	return True


def test_read_transform_rename() -> bool:
	column_mappings = list[ColumnMapping]()
	column_mappings.append(ColumnMapping(0, target_identifier="person_id"))
	column_mappings.append(ColumnMapping("Name", target_identifier="person_name"))
	column_mappings.append(ColumnMapping("name", target_identifier="person_name"))
	reader = TabularDataReader(column_mappings=column_mappings)
	df = reader.read_csv(f"{TEST_DATA_DIRECTORY}/sample01.csv")
	print(df)
	df.info()

	assert "person_id" in df, '"person_id" not in df'
	assert "person_name" in df, '"person_name" not in df'
	return True


def test_read_transform_rename_not_case_sensitive() -> bool:
	column_mappings = list[ColumnMapping]()
	column_mappings.append(ColumnMapping(0, target_identifier="person_id", name_is_case_sensitive=False))
	column_mappings.append(ColumnMapping("Name", target_identifier="person_name", name_is_case_sensitive=False))
	column_mappings.append(ColumnMapping(source_identifier_contains('FEMAL'), target_identifier="is_female", name_is_case_sensitive=False))
	column_mappings.append(ColumnMapping(source_identifier_starts_with('COMM'), target_identifier="_comment", name_is_case_sensitive=False))
	column_mappings.append(ColumnMapping(source_identifier_ends_with('DAY'), target_identifier="birth_day", name_is_case_sensitive=False))

	reader = TabularDataReader(column_mappings=column_mappings)
	df = reader.read_csv(f"{TEST_DATA_DIRECTORY}/sample01.csv")
	print(df)
	df.info()

	assert "person_id" in df, '"person_id" not in df'
	assert "person_name" in df, '"person_name" not in df'
	assert "is_female" in df, '"is_female" not in df'
	assert "_comment" in df, '"_comment" not in df'
	assert "birth_day" in df, '"birth_day" not in df'
	return True


def test_read_transform_rename_case_sensitive() -> bool:
	column_mappings = list[ColumnMapping]()
	column_mappings.append(ColumnMapping(0, target_identifier="person_id"))
	column_mappings.append(ColumnMapping("Name", target_identifier="person_name", name_is_case_sensitive=True))
	column_mappings.append(ColumnMapping(source_identifier_contains('FEMAL'), target_identifier="is_female"))
	column_mappings.append(ColumnMapping(source_identifier_starts_with('COMM'), target_identifier="_comment"))
	column_mappings.append(ColumnMapping(source_identifier_ends_with('DAY'), target_identifier="birth_day"))

	reader = TabularDataReader(column_mappings=column_mappings)
	df = reader.read_csv(f"{TEST_DATA_DIRECTORY}/sample01.csv")
	print(df)
	df.info()

	assert "person_name" not in df, '"person_name" in df'
	assert "is_female" not in df, '"is_female" in df'
	return True


def test_read_transform_rename_source_identifier_functions() -> bool:
	column_mappings = list[ColumnMapping]()
	column_mappings.append(ColumnMapping(source_identifier_contains('Femal'), target_identifier="is_female"))
	column_mappings.append(ColumnMapping(source_identifier_starts_with('comm'), target_identifier="_comment"))
	column_mappings.append(ColumnMapping(source_identifier_ends_with('day'), target_identifier="birth_day"))
	reader = TabularDataReader(column_mappings=column_mappings)
	df = reader.read_csv(f"{TEST_DATA_DIRECTORY}/sample01.csv")
	print(df)
	df.info()
	assert "is_female" in df, '"is_female" not in df'
	assert "_comment" in df, '"_comment" not in df'
	assert "birth_day" in df, '"birth_day" not in df'
	return True


def test_read_transform_rename_replace_na_with_none():
	reader = TabularDataReader()
	df = reader.read_csv(f"{TEST_DATA_DIRECTORY}/sample01.csv")
	print(df)
	df.info()
	assert np.nan not in df['comment'].tolist(), "There are nan values"
	return True


def test_read_transform_rename_do_not_replace_na_with_none():
	reader = TabularDataReader(data_settings=DataSettings(replace_na_with_none=False))
	df = reader.read_csv(f"{TEST_DATA_DIRECTORY}/sample01.csv")
	print(df)
	df.info()
	assert None not in df['comment'].tolist(), "There are None values"
	return True


def _read_spreadsheet(path: str, sheet_name: str | int | list | None = None) -> pd.DataFrame | dict[str | int, pd.DataFrame]:
	reader = TabularDataReader()
	spreadsheet = reader.read_spreadsheet(path, sheet_name)
	if isinstance(spreadsheet, pd.DataFrame):
		print(spreadsheet)
		spreadsheet.info()
	elif isinstance(spreadsheet, dict):
		for sheet_name, sheet in spreadsheet.items():
			print(sheet_name)
			print(sheet)
			sheet.info()
	return spreadsheet


def test_read_spreadsheet_ods():
	spreadsheet: dict[str, pd.DataFrame] = _read_spreadsheet(f"{TEST_DATA_DIRECTORY}/sample04.ods")
	assert isinstance(spreadsheet, dict), "Spreadsheet is not a dict"
	assert len(spreadsheet) == 2, "Spreadsheet does not have the expected number of sheets"
	return True


def test_read_spreadsheet_ods_one_sheet_by_position():
	spreadsheet: dict[str, pd.DataFrame] = _read_spreadsheet(f"{TEST_DATA_DIRECTORY}/sample04.ods", 1)
	assert isinstance(spreadsheet, pd.DataFrame), "Spreadsheet is not a data frame"
	return True


def test_read_spreadsheet_ods_two_sheets_by_position():
	spreadsheet: dict[str, pd.DataFrame] = _read_spreadsheet(f"{TEST_DATA_DIRECTORY}/sample04.ods", [0, 1])
	assert isinstance(spreadsheet, dict), "Spreadsheet is not a dict"
	assert len(spreadsheet) == 2, "Spreadsheet does not have the expected number of sheets"
	return True


def test_read_spreadsheet_ods_one_sheet_by_name():
	spreadsheet: dict[str, pd.DataFrame] = _read_spreadsheet(f"{TEST_DATA_DIRECTORY}/sample04.ods", 'sheet2')
	assert isinstance(spreadsheet, pd.DataFrame), "Spreadsheet is not a data frame"
	return True


def test_read_spreadsheet_ods_two_sheets_by_name():
	spreadsheet: dict[str, pd.DataFrame] = _read_spreadsheet(f"{TEST_DATA_DIRECTORY}/sample04.ods", ['sheet2', 'sheet1'])
	assert isinstance(spreadsheet, dict), "Spreadsheet is not a dict"
	assert len(spreadsheet) == 2, "Spreadsheet does not have the expected number of sheets"
	return True


def test_read_spreadsheet_excel():
	spreadsheet: dict[str, pd.DataFrame] = _read_spreadsheet(f"{TEST_DATA_DIRECTORY}/sample03.xlsx")
	assert isinstance(spreadsheet, dict), "Spreadsheet is not a dict"
	assert len(spreadsheet) == 2, "Spreadsheet does not have the expected number of sheets"
	return True


def test_read_spreadsheet_excel_one_sheet_by_position():
	spreadsheet: pd.DataFrame = _read_spreadsheet(f"{TEST_DATA_DIRECTORY}/sample03.xlsx", 1)
	assert isinstance(spreadsheet, pd.DataFrame), "Spreadsheet is not a data frame"
	return True


def test_read_spreadsheet_excel_two_sheets_by_position():
	spreadsheet: dict[str, pd.DataFrame] = _read_spreadsheet(f"{TEST_DATA_DIRECTORY}/sample03.xlsx", [0, 1])
	assert isinstance(spreadsheet, dict), "Spreadsheet is not a dict"
	assert len(spreadsheet) == 2, "Spreadsheet does not have the expected number of sheets"
	return True


def test_read_spreadsheet_excel_one_sheet_by_name():
	spreadsheet: pd.DataFrame = _read_spreadsheet(f"{TEST_DATA_DIRECTORY}/sample03.xlsx", 'sheet2')
	assert isinstance(spreadsheet, pd.DataFrame), "Spreadsheet is not a data frame"
	return True


def test_read_spreadsheet_excel_two_sheets_by_name():
	spreadsheet: dict[str, pd.DataFrame] = _read_spreadsheet(f"{TEST_DATA_DIRECTORY}/sample03.xlsx", ['sheet2', 'sheet1'])
	assert isinstance(spreadsheet, dict), "Spreadsheet is not a dict"
	assert len(spreadsheet) == 2, "Spreadsheet does not have the expected number of sheets"
	return True


def test_to_list_of_dict():
	spreadsheet: pd.DataFrame = _read_spreadsheet(f"{TEST_DATA_DIRECTORY}/sample04.ods", 'sheet1')
	spreadsheet: list[dict] = DataFrameTransformer.to_list_of_dict(spreadsheet)
	pp = PrettyPrinter(depth=4)
	pp.pprint(spreadsheet)
	assert isinstance(spreadsheet, list), "Not converted to a list"
	return True


def test_get_schema():
	spreadsheet: pd.DataFrame = _read_spreadsheet(f"{TEST_DATA_DIRECTORY}/sample04.ods", 'sheet1')
	x = DataFrameTransformer.get_schema(spreadsheet)
	pp = PrettyPrinter(depth=4)
	pp.pprint(x)
	return True


def test_remove_unwanted_columns_default():
	column_mappings = list[ColumnMapping]()
	column_mappings.append(ColumnMapping('isFemale', target_identifier="is_female"))
	reader = TabularDataReader(data_settings=DataSettings(), column_mappings=column_mappings)
	df = reader.read_csv(f"{TEST_DATA_DIRECTORY}/sample01.csv")
	print(df)
	df.info()
	assert len(df.columns) == 1
	return True


def test_remove_unwanted_columns_true():
	column_mappings = list[ColumnMapping]()
	column_mappings.append(ColumnMapping('isFemale', target_identifier="is_female"))
	reader = TabularDataReader(data_settings=DataSettings(remove_unwanted_columns=True), column_mappings=column_mappings)
	df = reader.read_csv(f"{TEST_DATA_DIRECTORY}/sample01.csv")
	print(df)
	df.info()
	assert len(df.columns) == 1
	return True


def test_remove_unwanted_columns_false():
	column_mappings = list[ColumnMapping]()
	column_mappings.append(ColumnMapping('isFemale', target_identifier="is_female"))
	reader = TabularDataReader(data_settings=DataSettings(remove_unwanted_columns=False), column_mappings=column_mappings)
	df = reader.read_csv(f"{TEST_DATA_DIRECTORY}/sample01.csv")
	print(df)
	df.info()
	assert len(df.columns) == 6
	return True


def test_remove_unwanted_columns_no_column_mapping():
	reader = TabularDataReader(data_settings=DataSettings(remove_unwanted_columns=True))
	df = reader.read_csv(f"{TEST_DATA_DIRECTORY}/sample01.csv")
	print(df)
	df.info()
	assert len(df.columns) == 6
	return True


def test_read_header(header, file_type, table_file):
	file_settings = FileSettings()
	if header != 'default':
		file_settings.header = header

	reader = TabularDataReader(file_settings=file_settings)

	if file_type == 'csv':
		df = reader.read_csv(table_file)
	else:
		df = reader.read_spreadsheet(table_file, 0)

	print(df)
	df.info()
	assert "ID" in df, "ID column not loaded"
	assert "name" in df, "name column not loaded"
	assert "bday" in df, "bday column not loaded"
	assert "isFemale" in df, "isFemale column not loaded"
	assert "height" in df, "height column not loaded"
	assert "comment" in df, "comment column not loaded"
	return True


def test_read_header_default_csv():
	table_file = f"{TEST_DATA_DIRECTORY}/sample01.csv"
	return test_read_header('default', 'csv', table_file)


def test_read_header_default_excel():
	table_file = f"{TEST_DATA_DIRECTORY}/sample03.xlsx"
	return test_read_header('default', 'excel', table_file)


def test_read_header_1_csv():
	table_file = f"{TEST_DATA_DIRECTORY}/sample05.csv"
	return test_read_header(1, 'csv', table_file)


def test_read_header_1_excel():
	table_file = f"{TEST_DATA_DIRECTORY}/sample06.xlsx"
	return test_read_header(1, 'excel', table_file)


def get_test_cases() -> dict[str, dict | Callable]:
	return {
		"reader": {
			"read": {
				"csv": test_read_csv,
				"pipe_sep_file": test_read_csv_pipe_separated,
				'header': {
					"default": {
						"csv": test_read_header_default_csv,
						"excel": test_read_header_default_excel
					},
					"1": {
						"csv": test_read_header_1_csv,
						"excel": test_read_header_1_excel
					}
				},
				"spreadsheet": {
					"excel": {
						"xlsx": test_read_spreadsheet_excel,
						"one_sheet": {
							"by_position": test_read_spreadsheet_excel_one_sheet_by_position,
							"by_name": test_read_spreadsheet_excel_one_sheet_by_name,
						},
						"two_sheets": {
							"by_position": test_read_spreadsheet_excel_two_sheets_by_position,
							"by_name": test_read_spreadsheet_excel_two_sheets_by_name,
						}
					},
					"ods": {
						"all_sheets": test_read_spreadsheet_ods,
						"one_sheet": {
							"by_position": test_read_spreadsheet_ods_one_sheet_by_position,
							"by_name": test_read_spreadsheet_ods_one_sheet_by_name,
						},
						"two_sheets": {
							"by_position": test_read_spreadsheet_ods_two_sheets_by_position,
							"by_name": test_read_spreadsheet_ods_two_sheets_by_name,
						}
					}
				},
				"transform": {
					"replace_na_with_none": {
						"True": test_read_transform_rename_replace_na_with_none,
						"False": test_read_transform_rename_do_not_replace_na_with_none
					},
					"rename": {
						"basic": test_read_transform_rename,
						"source_identifier_functions": test_read_transform_rename_source_identifier_functions,
						"not_case_sensitive": test_read_transform_rename_not_case_sensitive,
						"case_sensitive": test_read_transform_rename_case_sensitive,
					},
					"to_list_of_dict": test_to_list_of_dict,
					"get_schema": test_get_schema,
					"remove_unwanted_columns": {
						"default": test_remove_unwanted_columns_default,
						"True": test_remove_unwanted_columns_true,
						"False": test_remove_unwanted_columns_false,
						"no_column_mapping": test_remove_unwanted_columns_no_column_mapping
					}
				}
			}
		}
	}


def get_callables(d: dict, depth=0) -> list[Callable]:
	callables = list[Callable]()
	for k,v in sorted(d.items(),key=lambda x: x[0]):
		if isinstance(v, dict):
			callables.extend(get_callables(v, depth+1))
		elif isinstance(v, Callable):
			callables.append(v)
	return callables


def run(test_argv: list[str]):
	print("Running test for straw with args:")
	tests_to_run = test_argv[1:]
	pprint(tests_to_run)
	all_test_cases = benedict(get_test_cases())
	test_cases_to_run = list[Callable]()
	passed_count = 0
	failed_count = 0
	for test_id in tests_to_run:
		t = all_test_cases.get(test_id)
		if isinstance(t, Callable):
			test_cases_to_run.append(t)
		elif isinstance(t, dict):
			test_cases_to_run.extend(get_callables(t))

	for test_case in test_cases_to_run:
		passed = run_test_case(test_case)
		if passed:
			passed_count += 1
		else:
			failed_count += 1
	print(f"Test finished: {passed_count} PASSED, {failed_count} FAILED")


