from unittest import TestCase
from unittest.mock import patch
import json

from src import app


class TestApp(TestCase):
    def setUp(self):
        self.app = app.create_app()
        self.app.debug = True
        self.app.testing = True

    def tearDown(self):
        pass

    def test_stats_browser_endpoint_rejects_http_post_requests(self):
        api_client = self.app.test_client()
        response = api_client.post('/stats/browser')
        assert response.status_code == 405

    def test_stats_os_endpoint_rejects_http_post_requests(self):
        api_client = self.app.test_client()
        response = api_client.post('/stats/os')
        assert response.status_code == 405

    def test_stats_device_endpoint_rejects_http_post_requests(self):
        api_client = self.app.test_client()
        response = api_client.post('/stats/device')
        assert response.status_code == 405

    def test_stats_browser_rejects_http_post_requests_with_allow_header_in_response(self):
        api_client = self.app.test_client()
        response = api_client.post('/stats/browser')
        assert 'Allow' in response.headers

    def test_stats_os_rejects_http_post_requests_with_allow_header_in_response(self):
        api_client = self.app.test_client()
        response = api_client.post('/stats/os')
        assert 'Allow' in response.headers

    def test_stats_device_rejects_http_post_requests_with_allow_header_in_response(self):
        api_client = self.app.test_client()
        response = api_client.post('/stats/device')
        assert 'Allow' in response.headers

    def test_stats_browser_returns_bad_request_response_when_datetime_is_not_iso8601(self):
        api_client = self.app.test_client()
        response = api_client.get('/stats/browser?start_date=T17:01:01Z&end_date=2014-10-12T17:01:08Z')
        assert response.status_code == 400

    def test_stats_os_returns_bad_request_response_when_datetime_is_not_iso8601(self):
        api_client = self.app.test_client()
        response = api_client.get('/stats/os?start_date=T17:01:01Z&end_date=2014-10-12T17:01:08Z')
        assert response.status_code == 400

    def test_stats_device_returns_bad_request_response_when_datetime_is_not_iso8601(self):
        api_client = self.app.test_client()
        response = api_client.get('/stats/device?start_date=T17:01:01Z&end_date=2014-10-12T17:01:08Z')
        assert response.status_code == 400

    def test_stats_browser_returns_bad_request_response_when_start_date_and_end_date_are_not_the_only_request_arguments(
            self):
        api_client = self.app.test_client()
        response = api_client.get(
            '/stats/browser?start_date=2014-10-12T17:01:01Z&end_date=2014-10-12T17:01:08Z&alvaro=collantes')
        assert response.status_code == 400

    def test_stats_os_returns_bad_request_response_when_start_date_and_end_date_are_not_the_only_request_arguments(self):
        api_client = self.app.test_client()
        response = api_client.get(
            '/stats/os?start_date=2014-10-12T17:01:01Z&end_date=2014-10-12T17:01:08Z&alvaro=collantes')
        assert response.status_code == 400

    def test_stats_device_returns_bad_request_response_when_start_date_and_end_date_are_not_the_only_request_arguments(
            self):
        api_client = self.app.test_client()
        response = api_client.get(
            '/stats/device?start_date=2014-10-12T17:01:01Z&end_date=2014-10-12T17:01:08Z&alvaro=collantes')
        assert response.status_code == 400

    def test_stats_browser_returns_bad_request_response_when_just_start_date_or_end_date_are_indicated_as_arguments(
            self):
        api_client = self.app.test_client()
        response = api_client.get('/stats/browser?end_date=2014-10-12T17:01:01Z')
        assert response.status_code == 400

    def test_stats_os_returns_bad_request_response_when_just_start_date_or_end_date_are_indicated_as_arguments(self):
        api_client = self.app.test_client()
        response = api_client.get('/stats/os?end_date=2014-10-12T17:01:01Z')
        assert response.status_code == 400

    def test_stats_device_returns_bad_request_response_when_just_start_date_or_end_date_are_indicated_as_arguments(self):
        api_client = self.app.test_client()
        response = api_client.get('/stats/device?end_date=2014-10-12T17:01:01Z')
        assert response.status_code == 400

    def test_stats_browser_returns_bad_request_response_when_arguments_indicated_are_not_start_date_and_end_date(self):
        api_client = self.app.test_client()
        response = api_client.get('/stats/browser?alvaro=2014-10-12T17:01:01Z&collantes=2014-10-12T17:01:08Z')
        assert response.status_code == 400

    def test_stats_os_returns_bad_request_response_when_arguments_indicated_are_not_start_date_and_end_date(self):
        api_client = self.app.test_client()
        response = api_client.get('/stats/os?alvaro=2014-10-12T17:01:01Z&collantes=2014-10-12T17:01:08Z')
        assert response.status_code == 400

    def test_stats_device_returns_bad_request_response_when_arguments_indicated_are_not_start_date_and_end_date(self):
        api_client = self.app.test_client()
        response = api_client.get('/stats/device?alvaro=2014-10-12T17:01:01Z&collantes=2014-10-12T17:01:08Z')
        assert response.status_code == 400

    @patch('src.database_connection.query_table')
    def test_stats_browser_returns_not_found_response_when_events_db_returns_no_hits(self, mock):
        mock.return_value = None

        api_client = self.app.test_client()
        response = api_client.get('/stats/browser?start_date=2014-10-12T17:01:01Z&end_date=2014-10-12T17:01:08Z')
        assert response.status_code == 404

    @patch('src.database_connection.query_table')
    def test_stats_os_returns_not_found_response_when_events_db_returns_no_hits(self, mock):
        mock.return_value = None

        api_client = self.app.test_client()
        response = api_client.get('/stats/os?start_date=2014-10-12T17:01:01Z&end_date=2014-10-12T17:01:08Z')
        assert response.status_code == 404

    @patch('src.database_connection.query_table')
    def test_stats_device_returns_not_found_response_when_events_db_returns_no_hits(self, mock):
        mock.return_value = None

        api_client = self.app.test_client()
        response = api_client.get('/stats/device?start_date=2014-10-12T17:01:01Z&end_date=2014-10-12T17:01:08Z')
        assert response.status_code == 404

    @patch('src.database_connection.query_table')
    def test_stats_browser_returns_browsers_breakdown_from_db_filtered_by_indicated_time_frame_as_json_payload(self, mock):
        mock.return_value = [["Mobile Safari", "32.74%"], ["Safari", "22.12%"], ["IE", "14.16%"]]

        api_client = self.app.test_client()
        response = api_client.get('/stats/browser?start_date=2014-10-12T17:01:01Z&end_date=2014-10-12T17:01:08Z')
        assert json.loads(response.data) == mock.return_value

    @patch('src.database_connection.query_table')
    def test_stats_os_returns_os_breakdown_from_db_filtered_by_indicated_time_frame_as_json_payload(self, mock):
        mock.return_value = [["Windows", "32.74%"], ["iOS", "32.74%"], ["Mac OS X", "23.01%"]]

        api_client = self.app.test_client()
        response = api_client.get('/stats/os?start_date=2014-10-12T17:01:01Z&end_date=2014-10-12T17:01:08Z')
        assert json.loads(response.data) == mock.return_value

    @patch('src.database_connection.query_table')
    def test_stats_device_returns_device_breakdown_from_db_filtered_by_indicated_time_frame_as_json_payload(self, mock):
        mock.return_value = [["PC", "57.52%"], ["iPhone", "17.7%"], ["iPad", "15.04%"]]

        api_client = self.app.test_client()
        response = api_client.get('/stats/device?start_date=2014-10-12T17:01:01Z&end_date=2014-10-12T17:01:08Z')
        assert json.loads(response.data) == mock.return_value

    @patch('src.database_connection.query_table')
    def test_stats_browser_returns_browsers_breakdown_from_db_as_json_payload(self, mock):
        mock.return_value = [["Mobile Safari", "32.74%"], ["Safari", "22.12%"], ["IE", "14.16%"]]

        api_client = self.app.test_client()
        response = api_client.get('/stats/browser')
        assert json.loads(response.data) == mock.return_value

    @patch('src.database_connection.query_table')
    def test_stats_os_returns_os_breakdown_from_db_as_json_payload(self, mock):
        mock.return_value = [["Windows", "32.74%"], ["iOS", "32.74%"], ["Mac OS X", "23.01%"]]

        api_client = self.app.test_client()
        response = api_client.get('/stats/os')
        assert json.loads(response.data) == mock.return_value

    @patch('src.database_connection.query_table')
    def test_stats_device_returns_devices_breakdown_from_db_as_json_payload(self, mock):
        mock.return_value = [["Mobile Safari", "32.74%"], ["Safari", "22.12%"], ["IE", "14.16%"]]

        api_client = self.app.test_client()
        response = api_client.get('/stats/device')
        assert json.loads(response.data) == mock.return_value

