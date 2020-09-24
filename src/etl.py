import pandas as pd
from pandarallel import pandarallel
from src.transform_ip import parse_ip
from src.transform_ua import parse_user_agent_string
from src import extract_file, database_connection, app
import click
from datetime import datetime
import logging

logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Disabling SettingWithCopyWarning since I know I am operating over a copy of the df
pd.options.mode.chained_assignment = None


@click.command()
@click.option('--stdout', '-s', is_flag=True, help='Print the Top 5 Countries, Cities, Browsers, OS’s to standard out')
@click.option('--api', '-a', is_flag=True, help='Prepare the data to be consumed by the API')
def main(stdout, api):
    """Handles the control flow of the etl through cli arguments. Adds to the main dataframe the parsed fields:
    country, city, browser, os and device
    """
    # Pandas parallelization initialization
    pandarallel.initialize()

    if stdout:
        startTime = datetime.now()

        df = extract_file.get_file()
        if df is not None:
            logging.info('File extracted and loaded into a dataframe.')

            parsed_ua_df, top_browsers_sorted_lst, top_os_sorted_lst = parse_user_agent_string(df)
            logging.info('Top 5 browsers and OS calculated.')

            df.drop(['user_agent_string'], axis=1, inplace=True)
            df[['device', 'os', 'browser']] = parsed_ua_df[['device', 'os', 'browser']]

            parsed_ip_df, top_countries_sorted_lst, top_cities_sorted_lst = parse_ip(df)
            logging.info('Top 5 countries and cities calculated.')

            df.drop(['ip'], axis=1, inplace=True)
            df[['country', 'city']] = parsed_ip_df[['country', 'city']]

            click.echo('\nTop 5 browsers based on num of unique users:\n')
            click.echo('\n'.join([i[0] for i in top_browsers_sorted_lst]))
            click.echo('\nTop 5 OS based on num of unique users:\n')
            click.echo('\n'.join([i[0] for i in top_os_sorted_lst]))
            click.echo('\nTop 5 countries based on num of events:\n')
            click.echo('\n'.join([i[1] for i in top_countries_sorted_lst]))
            click.echo('\nTop 5 cities based on num of events:\n')
            click.echo('\n'.join([i[1] for i in top_cities_sorted_lst]))

            print('\n', datetime.now() - startTime)
    if api:
        startTime = datetime.now()

        logging.info('Preparing the data to be consumed by the API ...')

        df = extract_file.get_file()
        if df is not None:
            logging.info('File extracted.')

            parsed_ua_df = parse_user_agent_string(df, True)
            df.drop(['user_agent_string'], axis=1, inplace=True)
            df[['device', 'os', 'browser']] = parsed_ua_df[['device', 'os', 'browser']]

            parsed_ip_df = parse_ip(df, True)

            df.drop(['ip'], axis=1, inplace=True)
            df[['country', 'city']] = parsed_ip_df[['country', 'city']]

            print('Total number of lines in the file: ', df.shape[0])
            df.drop_duplicates(keep='last', inplace=True)
            print('Total number of lines in the file after removing duplicates: ', df.shape[0])

            database_connection.main(df)

            print('\n', datetime.now() - startTime)

            app.create_app().run()


if __name__ == '__main__':
    main()
