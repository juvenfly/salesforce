import sys
import time
import datetime
import urllib

import requests


# create an exception when a mass update has happened in SF. We don't want to process their entire DB.
class MassUpdateException(Exception):
    pass


# TODO: Use requests.Session() to simplify most of these requests.
class Connection(object):
    def __init__(self, credentials, obj_endpoint, api_version):
        """
        Initialize SF Connection object
        :credentials: Property credentials passed from ImportScript.get_property_credentials
        :config: CONFIG from property_sf_attributes.py
        """
        self.auth_url = "https://login.salesforce.com/services/oauth2/token"
        self.credentials = credentials
        self.instance_url = credentials['domain']
        self.access_token = credentials['access_token']
        self.api_version = api_version
        self.obj_endpoint = obj_endpoint

        self._refresh_token()
        self._get_date_range()

    def query_recent_records(self):
        """
        Returns list of salesforce_ids for records modified in the past 24 hours. Handles paginated responses.
        :return: List of salesforce_ids
        """
        contacts = []
        more_records_to_pull = True

        # TODO: This is gross...
        url = self.instance_url + '/services/data/v{}/sobjects/{}/updated/?start={}&end={}'.format(self.api_version,
                                                                                                   self.obj_endpoint,
                                                                                                   self.start_date,
                                                                                                   self.end_date)

        while more_records_to_pull:

            response = self.get_or_retry(url)

            if response:
                for i, salesforce_id in enumerate(response['ids']):
                    contacts.append(salesforce_id)

                if len(contacts) > 25000:
                    raise MassUpdateException('Mass update detected. Not processing entire SF database.')

                if 'nextRecordsUrl' in response:
                    next_records_url = response['nextRecordsUrl']
                    url = next_records_url

                else:
                    more_records_to_pull = False

        return contacts

    def create_sf_record(self, data):
        """
        Creates a new record in SF
        :param data: Data dictionary of SF field names and values
        :return: Returns True if response.json()['success'] == True, else False
        """
        max_retries = 3
        wait = 10

        url = self.instance_url + '/services/data/v{}/sobjects/{}/'.format(self.api_version, self.obj_endpoint)
        for n in range(max_retries):
            try:
                response = requests.post(url, data, headers={"Authorization": "Bearer {}".format(self.access_token)})
                response.raise_for_status()
                return response.json()['success']
            except requests.exceptions.Timeout as e:
                if n == max_retries - 1:
                    raise e
                wait *= (n + 1)
                time.sleep(wait)

    def update_sf_record(self, salesforce_id, data):
        """
        Update an individual record in SF
        :param salesforce_id: Unique ID for SF record
        :return: Returns True if no error codes received
        """
        max_retries = 3
        wait = 10

        url = self.instance_url + '/services/data/v{}/sobjects/{}/'.format(self.api_version, self.obj_endpoint) + salesforce_id
        for n in range(max_retries):
            try:
                response = requests.patch(url, data, headers={"Authorization": "Bearer {}".format(self.access_token)})
                response.raise_for_status()
                return True
            except requests.exceptions.Timeout as e:
                if n == max_retries - 1:
                    raise e
                wait *= (n + 1)
                time.sleep(wait)

    def query_single_object(self, salesforce_id):
        """
        Query an individual object within SF (e.g. a single Lead or Contact)
        :param salesforce_id: Unique ID for SF record
        :return: Returns response object if 'Id' in response, otherwise no return
        """
        url = self.instance_url + '/services/data/v{}/sobjects/{}/'.format(self.api_version, self.obj_endpoint) + salesforce_id
        response = self.get_or_retry(url)
        if response and 'Id' in response:
            return response
        else:
            return None

    def get_or_retry(self, url):
        """
        Query SF API with URL passed from other methods. Retries up to 3 times then throws Timeout if no response
        :param url: Fully formatted SF REST API call url
        :return: Returns response from SF API unless exception is thrown
        """
        max_retries = 3
        wait = 10
        counter = 1

        for n in range(max_retries):
            try:
                response = requests.get(url, headers={"Authorization": "Bearer {}".format(self.access_token)}, verify=True)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.Timeout as e:
                # retry 3 times in case of Timeout
                if n == max_retries - 1:
                    raise e
                wait = wait ** counter
                counter += 1
                time.sleep(wait)

            except requests.exceptions.MissingSchema:
                return None

            except requests.exceptions.HTTPError as e:
                if response.status_code == 401:
                    sys.stdout.write('refreshing token...')
                    self._refresh_token()
                else:
                    sys.stderr.write(response.json())
                    raise e

    def _refresh_token(self):
        """
        SF access tokens expire after 2 hours. This method refreshes self.access_token
        Throws KeyError if response.json() does not include access_token
        """
        payload = {
            'grant_type': 'password',
            'client_id': self.credentials['client_id'],  # consumer_key
            'client_secret': self.credentials['client_secret'],  # consumer_secret
            'username': self.credentials['user_key'],  # user_name
            'password': self.credentials['refresh_token'] + self.access_token  # password + refresh_token
        }

        r = requests.post(self.auth_url,
                          headers={"Content-Type": "application/x-www-form-urlencoded"},
                          data=payload)
        self.access_token = r.json()['access_token']

    def _get_date_range(self):
        """
        Internal method used to assign self.start_date and self.end_date
        These dates are used to find SF records modified in the past 24 hours
        """
        now = datetime.datetime.now()
        end_date = now.isoformat()
        start_date = (now - datetime.timedelta(days=1)).isoformat()

        self.start_date = self._format_date(start_date)
        self.end_date = self._format_date(end_date)

    @staticmethod
    def _format_date(date):
        """
        Internal method for formatting SF API friendly date strings
        :param date: datetime object, iso format
        :return: Returns properly formatted date
        """
        result = urllib.quote_plus(date.split('.', 1)[0]) + 'Z'
        return result
