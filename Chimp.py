# -*- coding: utf-8 -*-
"""
Created on Mon May 21 15:22:00 2018
@author: jeramie.goodwin

TODO:
* add api key as a parameter in mailchimp
* add adjustable offsets and limits to get_campaign & get_email_activity
"""
import sys
from urllib import parse
import requests
import json
import pandas as pd

# sys.path.append(r'..\')
class MailChimp:
    def __init__(self,version=3.0, apikey=None):
        # API key
        if apikey is None:
            try:
                f = open(r".\llaves.txt","r+") ##ENTER YOUR KEY HERE
                apikey = f.read().strip()
                f.close
            except FileExistsError:
                print(FileExistsError)

        parts = apikey.split('-')

        if len(parts) != 2:
            print("Invalid API key: " + apikey)
            print("Please enter a valid API key found on your MailChimp account")

            sys.exit()

        self.apikey = apikey
        self.shard = parts[1]
        self.version = str(version)
        self.api_root = "https://" + self.shard + ".api.mailchimp.com/" + self.version +"/"

    def get_json(self, params=None, dest=None):
        """
            This function takes as input an api key, root url, paramerters,
            and a destination.

            Returned is a JSON string from the MailChimp endpoint url with the fields
            prescribed in the API parameter dictionary (params)

            INPUT:
                apikey {str} = MailChimp user key
                root {url-str} = API GET call root url
                params {dict} = Query parameters and fields passed to the API call
                dest = {url-str} = Page/form destination

            OUTPUT:
                JSON
            """
        endpoint = parse.urljoin(self.api_root,dest)

        if params is not None:

            response = requests.get(endpoint, params=params,
                                    auth=("apikey",self.apikey),
                                    verify=True)
            try:
                response.raise_for_status()
                body = response.json()
                print("Success!")

            except requests.exceptions.HTTPError as err:
                print("Error: {} {}").format(str(response.status_code), err)
                print(json.dumps(response.json(), indent=4))

            except ValueError:
                print("Cannot decode json, got %s" )% response.text

            if len(body[dest]) == 0:

                print('Nothing found')

        else:
            response = requests.get(endpoint,
                                    auth=("apikey",self.apikey),
                                    verify=True)
            try:
                response.raise_for_status()
                body = response.json()
                print("Success!")

            except requests.exceptions.HTTPError as err:
                print("Error: {} {}").format(str(response.status_code), err)
                print(json.dumps(response.json(), indent=4))

            except ValueError:
                print("Cannot decode json, got %s" )% response.text

            if len(body[dest]) == 0:

                print('Nothing found')

        return body

    def get_campaign(self,dest='campaigns'):
        """
            This function returns a list of all campign ids associated with the
            apikey.
            ARGS:
                apikey {str} = MailChimp user key
                params {dict} = Query parameters and fields passed to the API call
                dest = {url-str} = Page/form destination

            OUTPUT:
                Pandas data frame -
                campaign_id, send_time, campaign _title
            """
            
        params={'fields':'campaigns.id,\
                          campaigns.settings.title,\
                          campaigns.send_time'}
        
        r = self.get_json(params=params,dest=dest)
        camps_dict = r[dest][0:]

        camp_lst = []

        for i in camps_dict:
            camp_df = pd.DataFrame.from_records(i)
            camp_lst.append(camp_df)

        cols = ['campaign_id', 'send_time','title']
        camp_info = pd.concat(camp_lst)
        camp_info.reset_index(drop=True, inplace=True)
        camp_info.columns = cols

        return camp_info

    def get_email_activity(self,camp_ids = [], count=500,dest='email_activity'):
        """
        This function returns a nested dictionary defined by the input paramaters
        from the URL endpoint.

        INPUT:
                apikey {str} = MailChimp user key
                params {dict} = Query parameters and fields passed to the API call
                camp_ids {tuple} = List of campaign ids returned
                                    from the get_campaign() function OR a Series

        OUTPUT:
                user_activity = Pandas dataframe of user activity organized by
                user email and campaing id

        """
        params={'count':count,
                                   'fields':'emails.campaign_id,\
                                    emails.email_address,'\
                                   'emails.activity,emails.activity.action'}
        activity = []
        camp_info = self.get_campaign()
        for i,c in enumerate(camp_ids):
            endpoint = self.apiroot+c+'/'+dest
            print(endpoint)
            r  = requests.get(endpoint,params=params,
                          auth=('apikey',self.apikey),
                          verify=True)

            try:
                r.raise_for_status()
                r = r.json()
                print("Success!")

            except requests.exceptions.HTTPError as err:
                print("Error: {} {}").format(str(r.status_code), err)
                print(json.dumps(r.json(), indent=4))

            except ValueError:
                print("Cannot decode json, got %s" )% r.text

            emails = r['emails']

            for j,user in enumerate(emails):
                if len(user['activity']) == 0:
                    df = pd.DataFrame(
                            {'action':'none',
                             'campaign_id':user['campaign_id'],
                             'email_address':user['email_address'],
                             'ip':' ',
                             'timestamp':camp_info['send_time'][i]}, index=[0])

                else:
                    df = pd.DataFrame.from_records(user)
                    df = pd.concat([df.drop('activity', axis=1),pd.DataFrame(df['activity'].tolist())], axis=1)

                activity.append(df)
        user_activity = pd.concat(activity)

        return user_activity
    
    def get_subscriber(self,count=500, list_id='190ffb16f9'):
        """ Gets member list stats and other information"""
        endpoint = parse.urljoin(self.apiroot,'lists/'+list_id+'/members')
        
        params = {'count':count,
                  'fields':'members.id,members.email_address,\
                   members.unique_email_id,members.status,members.stats,members.email_client,members.location'}
        
        r  = requests.get(endpoint,params=params,
                          auth=('apikey',self.apikey),
                          verify=True)
        r = r.json()
        members = r['members']
        df = pd.DataFrame(members)
        member_data = pd.concat([df.drop(['location','stats'],axis=1),
                       pd.DataFrame(df['location'].tolist()),
                       pd.DataFrame(df['stats'].tolist())],axis=1)
        return member_data
        
    
    
