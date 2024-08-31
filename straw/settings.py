from dataclasses import dataclass


class DataSettings:
	def __init__(self, replace_na_with_none: bool = True, remove_unwanted_columns: bool = True):
		self.replace_na_with_none = replace_na_with_none
		self.remove_unwanted_columns = remove_unwanted_columns


class FileSettings:
	def __init__(self, separator: str = ',', header: int | list[int] | None = 0):
		self.separator = separator
		self.header = header


class Default:
	DATA_SETTINGS = DataSettings()
	FILE_SETTINGS = FileSettings()
