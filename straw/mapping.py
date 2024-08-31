from dataclasses import dataclass
from typing import Callable
import pandas as pd

@dataclass
class ColumnMapping:
	source_identifier: int | str | tuple[Callable, str]
	target_identifier: str | None
	target_datatype: str | None
	name_is_case_sensitive: bool

	def __init__(
		self,
		source_identifier: int | str | tuple[Callable, str],
		target_identifier: str | None = None,
		name_is_case_sensitive: bool = True
	):
		self.source_identifier = source_identifier
		self.target_identifier = target_identifier
		self.name_is_case_sensitive = name_is_case_sensitive

	@staticmethod
	def source_identifier_starts_with(name: str) -> tuple[Callable, str]:
		return str.startswith, name

	@staticmethod
	def source_identifier_ends_with(name: str) -> tuple[Callable, str]:
		return str.endswith, name

	@staticmethod
	def source_identifier_contains(name: str) -> tuple[Callable, str]:
		return lambda column_name, param: param in column_name, name


