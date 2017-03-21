import json
import datetime

import httpretty
from nose.tools import assert_raises

import salesforce as SF

AUTH_URL = "https://login.salesforce.com/services/oauth2/token"
TEST_DOMAIN = "https://www.test_domain.com"

credentials = {
        'client_id': 'consumer_key',
        'client_secret': 'consumer_secret',
        'user_key': 'user_name',
        'refresh_token': 'password',
        'access_token': 'security_token',
        'domain': 'https://www.test_domain.com'
}

config = {
        'api_version': '1.0',
        'obj_endpoints': ['obj_endpoint']
}

test_connection = SF.Connection(credentials, config['obj_endpoints'][0], config['api_version'])


@httpretty.activate
def test_refresh_token():
    """
    Should get a new access_token or raise KeyError otherwise
    """
    httpretty.register_uri(httpretty.POST,
                           AUTH_URL,
                           responses=[
                               httpretty.Response(body=json.dumps({"access_token": "NEW_TOKEN"}), status=200),
                               httpretty.Response(body=json.dumps({"error": "error message"}), status=401)
                           ])

    test_connection.refresh_token()
    assert test_connection.access_token == "NEW_TOKEN"
    assert_raises(KeyError, test_connection.refresh_token)


@httpretty.activate
def test_get_or_retry():
    """
    Should return response JSON. If 401 returned, calls refresh_token. If Timeout, retry up to 2 more times.
    """
    httpretty.register_uri(httpretty.POST, AUTH_URL, body=json.dumps({"access_token": "NEW_TOKEN"}), status=200)

    httpretty.register_uri(httpretty.GET,
                           'http://www.test_domain.com',
                           responses=[
                               httpretty.Response(body=json.dumps({"salesforce_id": "1234", "data": "value"}), status=200),
                               httpretty.Response(body=json.dumps({"error": "bad token"}), status=401),
                           ])
    # test good GET
    response = test_connection.get_or_retry('http://www.test_domain.com')
    assert response == {"salesforce_id": "1234", "data": "value"}

    # test 401 handling
    test_connection.get_or_retry('http://www.test_domain.com')
    assert test_connection.access_token == "NEW_TOKEN"

    # TODO: test request timeouts


@httpretty.activate
def test_query_single_object():
    """
    Should return data for single SF object if it exists, else nothing
    """
    # first good call
    httpretty.register_uri(httpretty.GET,
                           TEST_DOMAIN + '/services/data/v1.0/sobjects/obj_endpoint/1234',
                           body=json.dumps({"Id": "1234", "data": "value1"}),
                           status=200)
    # second good call
    httpretty.register_uri(httpretty.GET,
                           TEST_DOMAIN + '/services/data/v1.0/sobjects/obj_endpoint/3456',
                           body=json.dumps({"Id": "3456", "data": "value2"}),
                           status=200)
    # call for nonexistant record
    httpretty.register_uri(httpretty.GET,
                           TEST_DOMAIN + '/services/data/v1.0/sobjects/obj_endpoint/5678',
                           body=json.dumps({"error": "no such record"}),
                           status=200)

    print TEST_DOMAIN + '/services/v1.0/sobjects/obj_endpoint/1234'
    response = test_connection.query_single_object("1234")
    assert response == {"Id": "1234", "data": "value1"}

    response = test_connection.query_single_object("3456")
    assert response == {"Id": "3456", "data": "value2"}
    #
    response = test_connection.query_single_object("5678")
    assert response is None


@httpretty.activate
def test_query_recent_records():
    """
    Should return list of salesforce_ids from response.
    If nextRecordsUrl present, get next pages of records until none left.
    """
    # test single page returned
    httpretty.register_uri(httpretty.GET,
                           TEST_DOMAIN + '/services/data/v1.0/sobjects/obj_endpoint/updated/?start={}&end={}'.format(test_connection.start_date, test_connection.end_date),
                           body=json.dumps({"ids": ["1234", "3456", "5678"]}),
                           status=200)

    contacts = test_connection.query_recent_records()
    assert contacts == ["1234", "3456", "5678"]

    # test multiple pages returned
    httpretty.register_uri(httpretty.GET,
                           TEST_DOMAIN + '/services/data/v1.0/sobjects/obj_endpoint/updated/?start={}&end={}'.format(test_connection.start_date, test_connection.end_date),
                           body=json.dumps({"ids": ["1234", "3456", "5678"], "nextRecordsUrl": "https://www.test_domain.com/nextRecordsUrl"}),
                           status=200)

    httpretty.register_uri(httpretty.GET,
                           TEST_DOMAIN + '/nextRecordsUrl',
                           responses=[
                           httpretty.Response(body=json.dumps({"ids": ["abcd", "defg", "ghij"]}), status=200),
                           httpretty.Response(body=None, status=404)
                           ])

    contacts = test_connection.query_recent_records()
    for contact in contacts:
        assert contact in ["1234", "3456", "5678", "abcd", "defg", "ghij"]

    # test mass update exception
    httpretty.register_uri(httpretty.GET,
                           TEST_DOMAIN + '/services/data/v1.0/sobjects/obj_endpoint/updated/?start={}&end={}'.format(test_connection.start_date, test_connection.end_date),
                           body=json.dumps({"ids": [str(x) for x in range(0, 30000)]}),
                           status=200)
    assert_raises(SF.MassUpdateException, test_connection.query_recent_records)


def test__get_date_range():
    """
    Should set self.start_date and self.end_date to past 24 hours
    """
    # initialize control times
    now = datetime.datetime.now()
    end_date = test_connection._format_date(now.isoformat())
    start_date = test_connection._format_date((now - datetime.timedelta(days=1)).isoformat())

    # set date range on test_connection
    test_connection._get_date_range()

    assert start_date == test_connection.start_date
    assert end_date == test_connection.end_date


def test__format_date():
    """
    Should return URL friendly date formats given datetime.isoformat()
    """
    outcomes = {
        datetime.datetime(2016, 1, 2, 3, 4, 5).isoformat(): "2016-01-02T03%3A04%3A05Z",
        datetime.datetime(2017, 2, 3, 4, 5, 6).isoformat(): "2017-02-03T04%3A05%3A06Z",
        datetime.datetime(2018, 12, 13, 14, 15, 16).isoformat(): "2018-12-13T14%3A15%3A16Z"
    }

    for date in outcomes:
        assert test_connection._format_date(date) == outcomes[date]
