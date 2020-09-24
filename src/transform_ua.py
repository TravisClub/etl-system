from user_agents import parse
import logging


def parse_user_agent_string(df, api=False):
    """Parses user agent string into device, browser and os.

    Parameters
    ----------
    df : pandas dataframe
        Dataframe that contains file rows without duplicates
    api : boolean
        Boolean flag used to prepare the data to be consumed by the api or not

    Returns
    -------
    user_agent_string_df : pandas dataframe
        Three columns dataframe; device, os and browser columns.
    top_browsers_sorted_lst : list
        The list of top 5 browsers based on num of unique users
    top_os_sorted_lst : list
        The list of top 5 OS based on num of unique users
    """
    logging.info('Parsing user agent strings ...')
    user_agent_string_df = df[['user_id', 'user_agent_string']]
    user_agent_string_df['user_agent_string'] = user_agent_string_df['user_agent_string'].parallel_apply(
        parse_ua_row)

    user_agent_string_df[['device', 'os', 'browser', 'na']] = user_agent_string_df.user_agent_string.str.split("/",
                                                                                                               expand=True)
    user_agent_string_df.drop(['user_agent_string', 'na'], axis=1, inplace=True)

    user_agent_string_df_no_user_duplicates = user_agent_string_df[['user_id', 'device', 'os', 'browser']]
    user_agent_string_df_no_user_duplicates.sort_values('user_id', inplace=True)
    logging.info('Total number of lines in the dataframe: {}'.format(user_agent_string_df_no_user_duplicates.shape[0]))
    user_agent_string_df_no_user_duplicates.drop_duplicates(subset='user_id', keep=False, inplace=True)
    logging.info('Total number of lines in the dataframe after removing user_id duplicates: {}'.format(
        user_agent_string_df_no_user_duplicates.shape[0]))

    if not api:
        top_browsers_sorted_lst, top_os_sorted_lst = count_browsers_os(user_agent_string_df_no_user_duplicates)

        return user_agent_string_df[['device', 'os', 'browser']], top_browsers_sorted_lst, top_os_sorted_lst
    else:
        return user_agent_string_df[['device', 'os', 'browser']]


def parse_ua_row(row):
    """Applies user_agents.parse() function to each user agent string row

    Parameters
    ----------
    row : str
        User agent string column row

    Returns
    -------
    row : str
        Parsed row into device, os and browser
    """
    return str(parse(row)).split('/')[0].strip() + '/' + parse(row).os.family + '/' + parse(row).browser.family


def count_browsers_os(df):
    """Counts the number of browsers and OSs.

    Parameters
    ----------
    df : pandas dataframe
        Dataframe that contains unique number of users

    Returns
    -------
    top_browsers_sorted_lst : list
        The list of top 5 browsers based on num of unique users
    top_os_sorted_lst : list
        The list of top 5 OS based on num of unique users
    """
    logging.info('Calculating Top 5 browsers and OS ...')

    top_browsers_sorted_lst = df['browser'].value_counts()[
                              :5].reset_index().values.tolist()
    top_os_sorted_lst = df['os'].value_counts()[
                        :5].reset_index().values.tolist()

    return top_browsers_sorted_lst, top_os_sorted_lst
