from docs import config
import sqlite3
from werkzeug.exceptions import InternalServerError


def create_database_connection(db_file):
    """Creates a database connection to the SQLite database specified by db_file

    Parameters
    ----------
    db_file : str
        database file

    Raises
    ------
    sqlite3.Error
        Error while connecting to the db.

    Returns
    -------
    conn : object
        connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except sqlite3.Error as e:
        print({'error': str(e)})

    return conn


def create_table(conn, df):
    """Creates events_log table

    Parameters
    ----------
    con : object
        connection object
    df : dataframe
        Pandas dataframe to load the table

    Raises
    ------
    sqlite3.Error
        Error while creating the table.
    """
    try:
        cur = conn.cursor()

        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='events_log'")
        if cur.fetchone() is None:
            # Creates table
            cur.execute(
                "CREATE TABLE IF NOT EXISTS events_log (raw_event TEXT NOT NULL UNIQUE,timestamps DATETIME,user_id TEXT,url TEXT,device TEXT,os TEXT,browser TEXT,country TEXT,city TEXT)")

        df.to_sql("events_log", conn, if_exists="replace")
    except sqlite3.Error as e:
        print({'error': str(e)})


def query_table(conn, breakdown, start_date=None, end_date=None):
    """Queries events_log table and gets the breakdown by browser, os or device for a given timeframe or for all data without filtering

    Parameters
    ----------
    breakdown : str
        browser, os or device to get the breakdown by
    start_date : datetime
        start date of the given timeframe or None - use ISO 8601 format, e.g. 2014-10-12T17:01:01Z.
    end_date : datetime
        start date of the given timeframe or None - use ISO 8601 format, e.g. 2014-10-12T17:01:01Z.

    Raises
    ------
    sqlite3.Error
        Error while connecting to the db.

    Returns
    -------
    query_result : str
        result of the query
    """
    try:
        cur = conn.cursor()

        if start_date is not None and end_date is not None:
            cur.execute(
                "SELECT {metric}, ROUND({metric}_count/(SELECT CAST(COUNT(*) AS float) FROM events_log WHERE timestamp > '{start_date}' AND timestamp < '{end_date}') * 100.0, 2) || '%' AS percentage "
                "FROM (SELECT {metric}, COUNT(*) AS {metric}_count FROM events_log WHERE timestamp > '{start_date}' AND timestamp < '{end_date}' GROUP BY {metric} ORDER BY {metric}_count DESC)".format(
                    metric=breakdown, start_date=start_date, end_date=end_date))
            query_result = cur.fetchall()
            conn.close()

        else:
            cur.execute(
                "SELECT {metric}, ROUND({metric}_count/(SELECT CAST(COUNT(*) AS float) FROM events_log) * 100.0, 2) || '%' AS percentage FROM (SELECT {metric}, COUNT(*) AS {metric}_count FROM events_log GROUP BY {metric} ORDER BY {metric}_count DESC)".format(
                    metric=breakdown))
            query_result = cur.fetchall()
            conn.close()

        if not query_result:
            return None
        else:
            return query_result

    except sqlite3.Error as e:
        print({'error': str(e)})
        raise InternalServerError('Querying data base failed.')


def main(df):
    """Calls create_table function if connection to database has been successful

    """
    # creates a database connection
    conn = create_database_connection(config.DB_FILE)

    # creates events_log table
    if conn is not None:
        create_table(conn, df)
        conn.close()
    else:
        print("Error! cannot create the database connection.")

