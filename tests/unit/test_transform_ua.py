from src import transform_ua


def test_parse_ua_row_returns_parsed_user_agent_string(some_user_agent_string):
    assert transform_ua.parse_ua_row(some_user_agent_string) == 'PC/Windows/IE'


def test_parse_ua_row_returns_other_string_when_user_agent_string_is_empty():
    assert transform_ua.parse_ua_row('') == 'Other/Other/Other'


def test_count_browsers_os_returns_browser_and_os_lists_when_df_indicated(some_browser_os_df):
    assert transform_ua.count_browsers_os(some_browser_os_df)[0] == [['Mobile Safari', 3], ['IE', 2], ['Chrome', 1]]
    assert transform_ua.count_browsers_os(some_browser_os_df)[1] == [['iOS', 3], ['Mac OS X', 2], ['Windows', 1]]

