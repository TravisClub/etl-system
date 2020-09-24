import pandas as pd
import requests
from io import BytesIO
import gzip
import logging
from docs import config


def get_file():
    """Grabs the file from the Google drive and loads it into a pandas dataframe. Parses date and time column into timestamp column

    Returns
    -------
    df : pandas dataframe
        dataframe that contains all file rows without duplicates
    """
    logging.info('Extracting file ...')
    file_id = config.ORIG_URL.split('/')[-2]
    dwn_url = 'https://drive.google.com/uc?export=download&id=' + file_id

    response = requests.get(dwn_url)
    if response.status_code != 200:
        logging.error('Error downloading file: {} {}'.format(response.status_code, response.content))
        return None
    else:
        df = open_gzip_read_tsv(BytesIO(response.content))

        logging.info('Total number of lines in the file: {}'.format(df.shape[0]))
        df.drop_duplicates(keep='last', inplace=True)
        logging.info('Total number of lines in the file after removing duplicates: {}'.format(df.shape[0]))
        # Added for events_log table PK
        df['raw_event'] = df[['date', 'time', 'user_id', 'url', 'ip', 'user_agent_string']].apply(lambda x: ''.join(x),
                                                                                                  axis=1)
        df['date'] = df['date'] + ' ' + df['time']
        df.rename(columns={'date': 'timestamp'}, inplace=True)
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S')
        df.drop(['time'], axis=1, inplace=True)

        return df


def open_gzip_read_tsv(bytes_io):
    """Opens gzip file and creates pandas dataframe

    Parameters
    ----------
    bytes_io : bytes-like object
        response content bytes

    Raises
    ------
    IOError
        Error while reading the file.

    Returns
    -------
    df : str
        pandas dataframe with the content of the tsv file
    """
    try:
        with gzip.open(bytes_io, 'rt') as read_file:
            df = pd.read_csv(read_file, sep='\t', names=['date', 'time', 'user_id', 'url', 'ip', 'user_agent_string'],
                             low_memory=False)
            return df
    except IOError as e:
        print("Error reading file: {}".format(e))
