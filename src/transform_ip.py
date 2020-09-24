from geolite2 import geolite2
import numpy as np
import logging


def parse_ip_to_country_city(ip):
    """Applies geolite2 parse function to each IP row and gets the city and country for every IP in that row.

    Parameters
    ----------
    ip : str
        IP or IPs separated by comma

    Raises
    ------
    ValueError
        If faulty IP value.
    KeyError
        If faulty key value.

    Returns
    -------
    row : str
        Parsed IP row into countries/cities or None
    """
    geo = geolite2.reader()

    if ',' in str(ip):
        countries_lst = list()
        cities_lst = list()
        for i in ip.split(','):
            try:
                x = geo.get(i.strip())
            except ValueError:
                return ''
            try:
                if x is not None:
                    if x.get('country', 0) != 0 and x.get('city', 0) != 0:
                        countries_lst.append(x['country']['names']['en'])
                        cities_lst.append(x['city']['names']['en'])
                    elif x.get('country', 0) != 0 and x.get('city', 0) == 0:
                        countries_lst.append(x['country']['names']['en'])
                    elif x.get('city', 0) != 0 and x.get('country', 0) == 0:
                        cities_lst.append(x['city']['names']['en'])
            except KeyError:
                return ''
        return '{}/{}'.format(','.join(countries_lst), ','.join(cities_lst))
    else:
        try:
            x = geo.get(str(ip))
        except ValueError:
            return ''
        try:
            if x is not None:
                if x.get('country', 0) != 0 and x.get('city', 0) != 0:
                    return '{}/{}'.format(x['country']['names']['en'], x['city']['names']['en'])
                elif x.get('city', 0) == 0:
                    return '{}'.format(x['country']['names']['en'])
                elif x.get('country', 0) == 0:
                    return '/{}'.format(x['city']['names']['en'])
        except KeyError:
            return ''


def parse_ip(df, api=False):
    """Parses IP into country and city.

    Parameters
    ----------
    df : pandas dataframe
        Dataframe that contains file rows without duplicates
    api : boolean
        Boolean flag used to prepare the data to be consumed by the api or not

    Returns
    -------
    ip_df : pandas dataframe
        Two columns dataframe; country and city columns.
    top_countries_sorted_lst : list
        The list of top 5 countries based on num of events
    top_cities_sorted_lst : list
        The list of top 5 cities based on num of events
    """
    logging.info('Parsing IPs ...')
    ip_df = df['ip'].to_frame()

    ip_df['country_city'] = ip_df['ip'].parallel_apply(parse_ip_to_country_city)
    ip_df[['country', 'city']] = ip_df.country_city.str.split("/", expand=True)

    ip_df.city.fillna(value=np.nan, inplace=True)
    ip_df['city'].replace(np.nan, "", inplace=True)

    if not api:
        top_countries_sorted_lst, top_cities_sorted_lst = count_countries_cities(ip_df)

        return ip_df[['country', 'city']], top_countries_sorted_lst, top_cities_sorted_lst
    else:
        return ip_df[['country', 'city']]


def count_countries_cities(df):
    """Counts the number of countries and cities.

    Parameters
    ----------
    df : pandas dataframe
        Dataframe that contains countries column and cities column

    Returns
    -------
    top_countries_sorted_lst : list
        The list of top 5 countries based on num of events
    top_cities_sorted_lst : list
        The list of top 5 cities based on num of events
    """
    logging.info('Calculating Top 5 countries and cities ...')
    countries_dct = dict()
    cities_dct = dict()

    for row in df.itertuples():
        if ',' in row.country:
            countries_dct[row.country.split(',')[0]] = countries_dct.get(row.country.split(',')[0], 0) + 1
            countries_dct[row.country.split(',')[1]] = countries_dct.get(row.country.split(',')[1], 0) + 1
        elif ',' in row.city:
            cities_dct[row.city.split(',')[0]] = cities_dct.get(row.city.split(',')[0], 0) + 1
            cities_dct[row.city.split(',')[1]] = cities_dct.get(row.city.split(',')[1], 0) + 1
        elif row.city != "":
            countries_dct[row.country] = countries_dct.get(row.country, 0) + 1
            cities_dct[row.city] = cities_dct.get(row.city, 0) + 1
        else:
            countries_dct[row.country] = countries_dct.get(row.country, 0) + 1

    top_countries_sorted_lst = sorted([(v, k) for k, v in countries_dct.items()], reverse=True)[:5]
    top_cities_sorted_lst = sorted([(v, k) for k, v in cities_dct.items()], reverse=True)[:5]

    return top_countries_sorted_lst, top_cities_sorted_lst
