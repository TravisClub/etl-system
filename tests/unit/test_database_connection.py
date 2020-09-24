from unittest import TestCase
import sqlite3
from src import database_connection
from unittest.mock import patch
import pandas as pd
import io


class TestDB(TestCase):
    events = {'timestamp': ['2014-10-12 17:01:01', '2014-10-12 17:01:05', '2014-10-12 17:01:06'],
              'user_id': ['f4fdd9e55192e94758eb079ec6e24b219fe7d71e', '0ae531264993367571e487fb486b13ea412aae3d',
                          'c5ac174ee153f7e570b179071f702bacfa347acf'],
              'url': ['http://741463b7c6f5cbf585841410229c67b52c9abd2b/bc70bcb60836146397b349088f14eaf0eb9eca39',
                      'http://38d6db9ae3170eaa9f3b0c27b77f1415a9f9afce/b017da16b32e98a2c5e5fe108b7ca48e8761411b',
                      'http://3ca8b6e4d6ab4fe1c94f3ac8bedef2ab98adb1d4/2d3419742e687465a810b09424a85806d541e805'],
              'raw_event': [
                  '2014-10-1319:45:498a586291477ba93d35561ee068de4b6e3ca61e1chttp://741463b7c6f5cbf585841410229c67b52c9abd2b/bc70bcb60836146397b349088f14eaf0eb9eca3986.40.128.3Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
                  '2014-10-1319:45:4945397851563344d0d5376c46db380ca16cb362ddhttp://38d6db9ae3170eaa9f3b0c27b77f1415a9f9afce/b017da16b32e98a2c5e5fe108b7ca48e8761411b94.14.226.156Mozilla/5.0 (Windows NT 5.1; rv:32.0) Gecko/20100101 Firefox/32.0',
                  '2014-10-1319:45:49fb9b90a68a530ede885c04efe18b056b29a56009http://3ca8b6e4d6ab4fe1c94f3ac8bedef2ab98adb1d4/2d3419742e687465a810b09424a85806d541e805194.81.33.57, 66.249.93.33Mozilla/5.0 (Linux; Android 4.2.1; Nexus 7 Build/JOP40D) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.102 Safari/537.36'],
              'device': ['iPad', 'iPad', 'Samsung GT-I9505'],
              'os': ['iOS', 'iOS', 'Android'],
              'browser': ['Mobile Safari', 'Mobile Safari', 'Chrome Mobile'],
              'country': ['United Kingdom', 'United Kingdom', 'United Kingdom'],
              'city': ['Wednesbury', 'Jarrow', 'Manchester']}

    df = pd.DataFrame(events, columns=['timestamp', 'user_id', 'url', 'raw_event', 'device', 'os', 'browser', 'country',
                                       'city'])

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")

    def tearDown(self):
        self.conn.close()

    def test_create_database_connection_returns_a_valid_sqlite3_connection_object(self):
        assert isinstance(database_connection.create_database_connection(":memory:"), sqlite3.Connection)

    def test_create_table_creates_events_log_table(self):
        database_connection.create_table(self.conn, self.df)
        c = self.conn.cursor()
        result = c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='events_log'")
        assert result.fetchone()[0] == 'events_log'

    def test_create_table_loads_it_with_df(self):
        database_connection.create_table(self.conn, self.df)
        c = self.conn.cursor()
        c.execute(
            "SELECT browser, ROUND(browser_count/(SELECT CAST(COUNT(*) AS float) FROM events_log) * 100.0, 2) || '%' AS percentage FROM (SELECT browser, COUNT(*) AS browser_count FROM events_log GROUP BY browser ORDER BY browser_count DESC)")

        result = c.fetchall()
        assert result[0] == ('Mobile Safari', '66.67%')

    def test_query_table_queries_table_when_no_time_frame_indicated(self):
        database_connection.create_table(self.conn, self.df)
        result = database_connection.query_table(self.conn, 'browser')

        assert result[0] == ('Mobile Safari', '66.67%')

    def test_query_table_queries_table_when_time_frame_indicated(self):
        database_connection.create_table(self.conn, self.df)
        result = database_connection.query_table(self.conn, 'browser', '2014-10-12 17:01:01', '2014-10-12 17:01:06')

        assert result[0] == ('Mobile Safari', '100.0%')

    def test_query_table_returns_none_when_no_events_inside_indicated_timeframe(self):
        database_connection.create_table(self.conn, self.df)
        result = database_connection.query_table(self.conn, 'browser', '2020-10-12 17:01:08', '2020-10-12 17:01:09')

        assert result is None

    @patch('src.database_connection.create_table')
    def test_main_calls_create_table_when_connection_is_not_none(self, mock_create_table):
        with patch('src.database_connection.create_database_connection'):
            database_connection.main(self.df)
            self.assertTrue(mock_create_table.called)

    @patch('sys.stdout', new_callable=io.StringIO)
    def test_main_prints_error_to_stdout_when_create_database_connection_is_none(self, mock_stdout):
        with patch('src.database_connection.create_database_connection') as mock_db_connection:
            mock_db_connection.return_value = None
            database_connection.main(self.df)
            self.assertEqual(mock_stdout.getvalue(), 'Error! cannot create the database connection.\n')
