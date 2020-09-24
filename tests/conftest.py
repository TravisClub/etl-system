import pytest
import pandas as pd


@pytest.fixture()
def some_raw_df():
    events = {'date': ['2014-10-12', '2014-10-12', '2014-10-12'],
              'time': ['17:01:01', '17:01:01', '17:01:01'],
              'user_id': ['f4fdd9e55192e94758eb079ec6e24b219fe7d71e', '0ae531264993367571e487fb486b13ea412aae3d',
                          'c5ac174ee153f7e570b179071f702bacfa347acf'],
              'url': ['http://741463b7c6f5cbf585841410229c67b52c9abd2b/bc70bcb60836146397b349088f14eaf0eb9eca39',
                      'http://38d6db9ae3170eaa9f3b0c27b77f1415a9f9afce/b017da16b32e98a2c5e5fe108b7ca48e8761411b',
                      'http://3ca8b6e4d6ab4fe1c94f3ac8bedef2ab98adb1d4/2d3419742e687465a810b09424a85806d541e805'],
              'ip': ['94.11.238.152', '92.238.71.109', '194.81.33.57, 66.249.93.33'],
              'user_agent_string': ['Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
                                    'Mozilla/5.0 (Windows NT 5.1; rv:32.0) Gecko/20100101 Firefox/32.0',
                                    'Mozilla/5.0 (Linux; Android 4.2.1; Nexus 7 Build/JOP40D) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.102 Safari/537.36']}

    df = pd.DataFrame(events, columns=['date', 'time', 'user_id', 'url', 'ip', 'user_agent_string'])

    yield df


@pytest.fixture()
def some_country_city_df():
    content = {'country': ['United Kingdom', 'United Kingdom', 'United Kingdom'],
               'city': ['Wednesbury', 'Jarrow', 'Manchester']}

    country_city_df = pd.DataFrame(content, columns=['country', 'city'])

    yield country_city_df


@pytest.fixture()
def some_countries_city_df():
    content = {'country': ['United Kingdom,Spain', 'United Kingdom,Spain', 'United Kingdom'],
               'city': ['Wednesbury', 'Jarrow', 'Manchester']}

    countries_city_df = pd.DataFrame(content, columns=['country', 'city'])

    yield countries_city_df


@pytest.fixture()
def some_country_cities_df():
    content = {'country': ['United Kingdom', 'United Kingdom', 'United Kingdom'],
               'city': ['Wednesbury,Valladolid', 'Jarrow,Valladolid', 'Manchester']}

    country_cities_df = pd.DataFrame(content, columns=['country', 'city'])

    yield country_cities_df


@pytest.fixture()
def some_browser_os_df():
    content = {'browser': ['Mobile Safari', 'Mobile Safari', 'Mobile Safari', 'IE', 'IE', 'Chrome'],
               'os': ['iOS', 'iOS', 'iOS', 'Mac OS X', 'Mac OS X', 'Windows']}

    browser_os_df = pd.DataFrame(content, columns=['browser', 'os'])

    yield browser_os_df


@pytest.fixture()
def some_empty_df():
    content = {}

    empty_df = pd.DataFrame(content)

    yield empty_df


@pytest.fixture()
def some_ip_list():
    ip_list = ['86.40.128.3', '94.14.226.156', '80.36.109.91']

    yield ip_list


@pytest.fixture()
def some_more_than_one_ip_list():
    more_than_one_ip_list = ['86.40.128.3', '94.14.226.156', '80.36.109.91,78.72.108.136']

    yield more_than_one_ip_list


@pytest.fixture()
def some_empty_ip_list():
    empty_ip_list = ['', '', '']

    yield empty_ip_list


@pytest.fixture()
def some_invalid_ip_list():
    invalid_ip_list = ['86.40.128', '86.40.128.3.344', '128.3']

    yield invalid_ip_list


@pytest.fixture()
def some_inappropriate_values_list():
    inappropriate_values_list = ['Alvaro', 12, True]

    yield inappropriate_values_list


@pytest.fixture()
def some_user_agent_string():
    ua = 'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko'

    yield ua
