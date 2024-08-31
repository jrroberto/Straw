import json
from collections import OrderedDict
from typing import Callable
import numpy as np
import pandas as pd

from straw.mapping import ColumnMapping
from straw.settings import DataSettings
from straw.settings import Default as DefaultSettings


class DataFrameTransformer:
	def __init__(
			self,
			column_mappings: list[ColumnMapping] | None = None,
			data_settings: DataSettings = DefaultSettings.DATA_SETTINGS,
	):
		self.column_mappings: list[ColumnMapping] = column_mappings if column_mappings is not None else list[
			ColumnMapping]()
		self.data_settings = data_settings

	def get_first_matching_column_name(
			self,
			df: pd.DataFrame,
			source_identifier: int | str | tuple[Callable, str],
			name_is_case_sensitive: bool
	) -> str | None:
		if isinstance(source_identifier, int):
			if len(df.columns) > source_identifier:
				return df.columns[source_identifier]
		elif isinstance(source_identifier, str):
			if name_is_case_sensitive:
				if source_identifier in df:
					return source_identifier
			elif not name_is_case_sensitive:
				for column_name in df.columns:
					if column_name.lower() == source_identifier.lower():
						return column_name
		elif isinstance(source_identifier, tuple):
			func = source_identifier[0]
			param = source_identifier[1] if name_is_case_sensitive else source_identifier[1].lower()
			for column_name in df.columns:
				name = column_name if name_is_case_sensitive else column_name.lower()
				if func(name, param):
					return column_name
		return None

	def transform(self, df: pd.DataFrame) -> pd.DataFrame:
		if self.data_settings.replace_na_with_none:
			df = df.replace(np.nan, None)

		for mapping in self.column_mappings:
			matching_column_name = self.get_first_matching_column_name(df, mapping.source_identifier,
																	   mapping.name_is_case_sensitive)
			if matching_column_name:
				if mapping.target_identifier:
					df = df.rename(columns={matching_column_name: mapping.target_identifier})

		if self.data_settings.remove_unwanted_columns and len(self.column_mappings) > 0:
			wanted_column_names = OrderedDict((x, 1) for x in [m.target_identifier for m in self.column_mappings if m.target_identifier in df]).keys()
			df = df[wanted_column_names]

		return df

	@staticmethod
	def to_list_of_dict(df: pd.DataFrame) -> list[dict]:
		return df.to_dict('records')

	@staticmethod
	def get_schema(df: pd.DataFrame) -> dict:
		return json.loads(df.to_json(orient="table")).get('schema')
