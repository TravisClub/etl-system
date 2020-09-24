from flask import Flask, request, Response
from werkzeug.exceptions import BadRequest, NotFound
from http import HTTPStatus
import json
import datetime
from src import database_connection
from docs import config


def create_app():
    application = Flask(__name__)

    @application.route('/stats/browser', methods=['GET'])
    def stats_browser():
        """
        ---
        get:
            summary: browsers breakdown
            description: Fetch a breakdown summary of all browsers for a given start and end date or for the whole table.
            produces:
            - "application/json"
            parameters:
            - name: start_date
              description: start date of the given timeframe - use ISO 8601 format, e.g. 2014-10-12T17:01:01Z.
              required: No
              type: datetime
            - name: end_date
              description: end date of the given timeframe - use ISO 8601 format, e.g. 2014-10-12T17:01:01Z.
              required: No
              type: datetime
            responses:
                200:
                    description: Browsers breakdown was successfully retrieved.
                    content:
                        application/json
                400:
                    description: The browser (or proxy) sent a request that this server could not understand.
                404:
                    description: Browsers breakdown not found.
                500:
                    description: Internal Server Error
                    content:
                        application/json
        """
        return get_stats('browser')

    @application.route('/stats/os', methods=['GET'])
    def stats_os():
        """
        ---
        get:
            summary: operating systems breakdown
            description: Fetch a breakdown summary of all OS's for a given start and end date or for the whole table.
            produces:
            - "application/json"
            parameters:
            - name: start_date
              description: start date of the given timeframe - use ISO 8601 format, e.g. 2014-10-12T17:01:01Z.
              required: No
              type: datetime
            - name: end_date
              description: end date of the given timeframe - use ISO 8601 format, e.g. 2014-10-12T17:01:01Z.
              required: No
              type: datetime
            responses:
                200:
                    description: OS's breakdown was successfully retrieved.
                    content:
                        application/json
                400:
                    description: The browser (or proxy) sent a request that this server could not understand.
                404:
                    description: OS's breakdown not found.
                500:
                    description: Internal Server Error
                    content:
                        application/json
        """
        return get_stats('os')

    @application.route('/stats/device', methods=['GET'])
    def stats_device():
        """
        ---
        get:
            summary: devices breakdown
            description: Fetch a breakdown summary of all devices for a given start and end date or for the whole table.
            produces:
            - "application/json"
            parameters:
            - name: start_date
              description: start date of the given timeframe - use ISO 8601 format, e.g. 2014-10-12T17:01:01Z.
              required: No
              type: datetime
            - name: end_date
              description: end date of the given timeframe - use ISO 8601 format, e.g. 2014-10-12T17:01:01Z.
              required: No
              type: datetime
            responses:
                200:
                    description: Devices breakdown was successfully retrieved.
                    content:
                        application/json
                400:
                    description: The browser (or proxy) sent a request that this server could not understand.
                404:
                    description: Devices breakdown not found.
                500:
                    description: Internal Server Error
                    content:
                        application/json
        """
        return get_stats('device')

    return application


def validate_timestamp(date_string):
    """Validates if a date is in the ISO 8601 format

    Parameters
    ----------
    date_string : str
        a date and time indicated as url string parameter

    Raises
    ------
    ValueError
        If datetime indicated is not valid datetime in the ISO 8601 format.
    """
    try:
        return datetime.datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%SZ").strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        print('{} is not valid datetime in the ISO 8601 format, e.g. 2020-02-13T20:56:34Z'.format(date_string))
        raise BadRequest("Incorrectly formatted day string - must be ISO 8601 compliant date")


def get_stats(breakdown_element):
    """Gets the disired stats and builds HTTP response

    Parameters
    ----------
    breakdown_element : str
        string indicating the desired element breakdown

    Raises
    ------
    BadRequest
        If just one datetime or a different argument is indicated.
    NotFound
        If no events found for the indicated timeframe.
    """
    if len(request.args) == 2 and 'start_date' in request.args and 'end_date' in request.args:
        start_date = validate_timestamp(request.args.get('start_date'))
        end_date = validate_timestamp(request.args.get('end_date'))
        result = database_connection.query_table(database_connection.create_database_connection(config.DB_FILE), breakdown_element, start_date, end_date)
    elif len(request.args) == 0:
        result = database_connection.query_table(database_connection.create_database_connection(config.DB_FILE), breakdown_element)
    else:
        print("Warning! Bad form content. Only one start_date and one end_date or none should be provided")
        raise BadRequest('Only one start_date and one end_date or none should be provided')

    if result is None:
        raise NotFound("No events found for indicated timeframe")
    else:
        return Response(json.dumps(result), status=HTTPStatus.OK, mimetype='application/json')


if __name__ == '__main__':
    create_app().run()
