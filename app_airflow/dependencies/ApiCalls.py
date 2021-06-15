import json
import sendgrid
import os
import sys
from urllib.parse import quote_plus
import urllib.parse
import urllib.request
from google.cloud import bigquery
from google.oauth2 import service_account


class APICalls:
    # Initializing our class where we'll have different functions working with the streamsets api
    def method(self):
        return 'instance method called', self

    @classmethod
    def urllib_sync_api_request(self, url, **kwargs):
        """
        This function makes a Syncronous request to an API Endpoint
        In the dag, if your api takes no parameters, then you should send
        the argument parameters = None
        If the response receives a 200 code, then the Task finishes successfully
        """
        try:
            with urllib.request.urlopen(url) as response:
                response = response.read()
                print(response)
                if '200' in str(response):
                    print("log: request done correctly")
                    return {"response_code":200}
        except Exception as e:
            print("log, this request failed")
            raise ValueError(e)
