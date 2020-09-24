from unittest import TestCase
from unittest.mock import patch, MagicMock
from src import extract_file
import pandas as pd


class Test(TestCase):
    @patch("gzip.open", side_effect=IOError("IOError"))
    def test_exception_raised_if_error_while_reading_gzip_file(self, mock):
        self.assertRaises(ValueError, extract_file.open_gzip_read_tsv('fake text'.encode()))


def test_get_file_returns_none_if_bad_download_url_provided():
    with patch('requests.get') as mock_request:
        mock_request.return_value.status_code = 404
        assert extract_file.get_file() is None


def test_get_file_returns_df_if_valid_file_in_drive(some_raw_df):
    mock_open_gz = MagicMock(name="df_generator")
    mock_open_gz.return_value = some_raw_df
    extract_file.open_gzip_read_tsv = mock_open_gz
    with patch('requests.get') as mock_request:
        mock_request.return_value.status_code = 200
        mock_request.return_value.content = "Fake content".encode()
        assert isinstance(extract_file.get_file(), pd.DataFrame)
