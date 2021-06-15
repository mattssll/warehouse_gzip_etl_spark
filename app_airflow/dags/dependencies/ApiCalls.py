import json
#import sendgrid
import os
import sys
import requests

class APICalls:
    # Initializing our class where we'll have different functions working with the streamsets api
    def method(self):
        return 'instance method called', self

    @classmethod
    def urllib_sync_api_request(self, url, **kwargs):
        r = requests.get(url)
        print("print response", json.dumps(r))
        return(json.dumps(r))
