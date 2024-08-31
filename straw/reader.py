from straw.settings import DataSettings, FileSettings
from straw.settings import Default as DefaultSettings
import pandas as pd

from straw.mapping import ColumnMapping
from straw.df_transform import DataFrameTransformer


class TabularDataReader:
	def __init__(
			self,
			data_settings: DataSettings = DefaultSettings.DATA_SETTINGS,
			file_settings: FileSettings = DefaultSettings.FILE_SETTINGS,
			column_mappings: list[ColumnMapping] | None = None
	):
		self.data_settings = data_settings
		self.file_settings = file_settings
		self.column_mappings = column_mappings

	def transform(self, frames: pd.DataFrame | dict[str | int, pd.DataFrame]) -> pd.DataFrame | dict[
		str | int, pd.DataFrame]:
		transformer = DataFrameTransformer(column_mappings=self.column_mappings, data_settings=self.data_settings)

		if isinstance(frames, pd.DataFrame):
			return transformer.transform(frames)

		x: dict[str, pd.DataFrame] = frames
		for frame_name, frame in x.items():
			frames[frame_name] = transformer.transform(frame)
		return frames

	def read_csv(
			self,
			filepath_or_buffer
	) -> pd.DataFrame:
		parameters = {"filepath_or_buffer": filepath_or_buffer, "sep": self.file_settings.separator,
					  'header': self.file_settings.header}
		df = pd.read_csv(**parameters)
		df = self.transform(df)
		return df

	def read_spreadsheet(
			self, io, sheet_name: str | int | list | None = None
	) -> pd.DataFrame | dict[str | int, pd.DataFrame]:
		parameters = {"io": io, 'sheet_name': sheet_name, 'header': self.file_settings.header}
		frames = pd.read_excel(**parameters)
		frames = self.transform(frames)
		return frames
