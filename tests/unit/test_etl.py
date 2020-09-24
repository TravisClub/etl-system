from src import etl
from click.testing import CliRunner
from unittest.mock import patch


@patch('src.extract_file.get_file')
@patch('src.transform_ua.parse_user_agent_string')
@patch('src.transform_ip.parse_ip')
def test_top_five_metrics_generated_when_stdout_cli_argument_indicated(mock_get_file, mock_parse_user_agent_string,
                                                                       mock_parse_ip):
    runner = CliRunner()
    result = runner.invoke(etl.main, ['--stdout', '-s'])
    assert result.exit_code == 0
    assert 'Top 5 cities based on num of events' in result.output


@patch('src.extract_file.get_file', return_value=None)
def test_top_five_metrics_not_generated_when_stdout_cli_argument_indicated_and_df_equal_none(mock_get_file):
    runner = CliRunner()
    result = runner.invoke(etl.main, ['--stdout', '-s'])
    assert result.exit_code == 0
    assert 'Top 5 cities based on num of events' not in result.output


@patch('src.extract_file.get_file', return_value=None)
def test_app_create_app_not_called_when_api_cli_argument_indicated_and_df_equal_none(mock_get_file):
    with patch('src.app.create_app') as mock_app_create_app:
        runner = CliRunner()
        result = runner.invoke(etl.main, ['--api', '-a'])
        assert result.exit_code == 0
        assert not mock_app_create_app.called


@patch('src.extract_file.get_file')
@patch('src.transform_ua.parse_user_agent_string')
@patch('src.transform_ip.parse_ip')
@patch('src.database_connection.main')
@patch('src.app.create_app')
def test_top_five_metrics_not_generated_when_api_cli_argument_indicated(mock_get_file, mock_parse_user_agent_string,
                                                                        mock_parse_ip, mock_database_connection,
                                                                        mock_app_create_app):
    runner = CliRunner()
    result = runner.invoke(etl.main, ['--api', '-a'])
    assert result.exit_code == 0
    assert 'Top 5 cities based on num of events' not in result.output


@patch('src.extract_file.get_file')
@patch('src.transform_ua.parse_user_agent_string')
@patch('src.transform_ip.parse_ip')
@patch('src.database_connection.main')
def test_app_create_app_not_called_when_stdout_cli_argument_indicated(mock_get_file, mock_parse_user_agent_string,
                                                                      mock_parse_ip, mock_database_connection):
    with patch('src.app.create_app') as mock_app_create_app:
        runner = CliRunner()
        result = runner.invoke(etl.main, ['--stdout', '-s'])
        assert result.exit_code == 0
        assert not mock_app_create_app.called


@patch('src.extract_file.get_file')
@patch('src.transform_ua.parse_user_agent_string')
@patch('src.transform_ip.parse_ip')
@patch('src.app.create_app')
def test_database_connection_not_called_when_stdout_cli_argument_indicated(mock_get_file, mock_parse_user_agent_string,
                                                                           mock_parse_ip, mock_app_create_app):
    with patch('src.database_connection.main') as mock_database_connection:
        runner = CliRunner()
        result = runner.invoke(etl.main, ['--stdout', '-s'])
        assert result.exit_code == 0
        assert not mock_database_connection.called


@patch('src.extract_file.get_file')
@patch('src.transform_ua.parse_user_agent_string')
@patch('src.transform_ip.parse_ip')
@patch('src.app.create_app')
def test_database_connection_called_when_api_cli_argument_indicated(mock_get_file, mock_parse_user_agent_string,
                                                                    mock_parse_ip, mock_app_create_app):
    with patch('src.database_connection.main') as mock_database_connection:
        runner = CliRunner()
        result = runner.invoke(etl.main, ['--api', '-a'])
        assert result.exit_code == 0
        mock_database_connection.assert_called_once()


@patch('src.extract_file.get_file')
@patch('src.transform_ua.parse_user_agent_string')
@patch('src.transform_ip.parse_ip')
@patch('src.database_connection.main')
def test_app_create_app_called_when_api_cli_argument_indicated(mock_get_file, mock_parse_user_agent_string,
                                                               mock_parse_ip, mock_database_connection):
    with patch('src.app.create_app') as mock_app_create_app:
        runner = CliRunner()
        result = runner.invoke(etl.main, ['--api', '-a'])
        assert result.exit_code == 0
        mock_app_create_app.assert_called_once()
