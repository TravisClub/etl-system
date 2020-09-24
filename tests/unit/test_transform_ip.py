from src import transform_ip


def test_count_countries_cities_returns_top_five_countries_and_cities_lists_when_dataframe_indicated(
        some_country_city_df):
    assert transform_ip.count_countries_cities(some_country_city_df)[0] == [(3, 'United Kingdom')]
    assert transform_ip.count_countries_cities(some_country_city_df)[1] == [(1, 'Wednesbury'), (1, 'Manchester'),
                                                                            (1, 'Jarrow')]


def test_count_countries_cities_returns_top_five_countries_list_when_dataframe_wiht_more_than_one_country_in_same_row_indicated(
        some_countries_city_df):
    assert transform_ip.count_countries_cities(some_countries_city_df)[0] == [(3, 'United Kingdom'), (2, 'Spain')]


def test_count_countries_cities_returns_top_five_cities_list_when_dataframe_wiht_more_than_one_city_in_same_row_indicated(
        some_country_cities_df):
    assert transform_ip.count_countries_cities(some_country_cities_df)[1] == [(2, 'Valladolid'), (1, 'Wednesbury'),
                                                                              (1, 'Manchester'), (1, 'Jarrow')]


def test_count_countries_cities_returns_top_five_countries_and_cities_empty_lists_when_empty_dataframe_indicated(
        some_empty_df):
    assert transform_ip.count_countries_cities(some_empty_df)[0] == []
    assert transform_ip.count_countries_cities(some_empty_df)[1] == []


def test_parse_ip_to_country_city_parses_ip_to_country_city(some_ip_list):
    countries_cities_list = list()
    for ip in some_ip_list:
        countries_cities_list.append(transform_ip.parse_ip_to_country_city(ip))
    assert countries_cities_list == ['Ireland/Edgeworthstown', 'United Kingdom/Greenwich', 'Spain/Almuñécar']


def test_parse_ip_to_country_city_parses_ip_to_country_city_when_more_than_one_ip_indicated_in_the_same_row(
        some_more_than_one_ip_list):
    countries_cities_list = list()
    for ip in some_more_than_one_ip_list:
        countries_cities_list.append(transform_ip.parse_ip_to_country_city(ip))
    assert countries_cities_list == ['Ireland/Edgeworthstown', 'United Kingdom/Greenwich',
                                     'Spain,Sweden/Almuñécar,Lindesberg']


def test_parse_ip_to_country_city_parses_ip_to_country_city_when_more_than_one_ip_indicated_in_the_same_row_and_one_of_them_raises_value_error(
        some_more_than_one_ip_list):
    countries_cities_list = list()
    for ip in some_more_than_one_ip_list:
        countries_cities_list.append(transform_ip.parse_ip_to_country_city(ip))
    assert countries_cities_list == ['Ireland/Edgeworthstown', 'United Kingdom/Greenwich',
                                     'Spain,Sweden/Almuñécar,Lindesberg']


def test_parse_ip_to_country_city_returns_empty_country_city_if_empty_ip_indicated(some_empty_ip_list):
    countries_cities_list = list()
    for ip in some_empty_ip_list:
        countries_cities_list.append(transform_ip.parse_ip_to_country_city(ip))
    assert countries_cities_list == ['', '', '']


def test_parse_ip_to_country_city_returns_empty_country_city_if_not_valid_ip_indicated(some_invalid_ip_list):
    countries_cities_list = list()
    for ip in some_invalid_ip_list:
        countries_cities_list.append(transform_ip.parse_ip_to_country_city(ip))
    assert countries_cities_list == ['', '', '']


def test_parse_ip_to_country_city_returns_empty_country_city_if_inappropriate_value_indicated(
        some_inappropriate_values_list):
    countries_cities_list = list()
    for ip in some_inappropriate_values_list:
        countries_cities_list.append(transform_ip.parse_ip_to_country_city(ip))
    assert countries_cities_list == ['', '', '']
