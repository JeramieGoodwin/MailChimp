# -*- coding: utf-8 -*-
"""
Created on Mon May 21 15:22:00 2018
@author: jeramie.goodwin

TODO:
    * Speed up email acitivity module
        > Use generator?
        > batch process?
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

    def get_json(self,dest_key=None,params=None,endpoint=None):
        """
            ARGS:
                apikey {str} = MailChimp user key
                params {dict} = Query parameters and fields passed to the API call
                endpoint {str}
                key {str}

            OUTPUT:
                JSON
            """
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
        
        data = body[dest_key]

        return data

    def get_campaign(self,dest='campaigns',
                     params={'fields':'campaigns.id,campaigns.settings.title,campaigns.send_time'}):
        """
            ARGS:
                apikey {str} = MailChimp user key
                params {dict} = Query parameters and fields passed to the API call
                dest = {url-str} = Page/form destination

            OUTPUT:
                Pandas data frame -
                campaign_id, send_time, campaign _title
            """
        ep = parse.urljoin(self.api_root,dest)

        camps_dict = self.get_json(params=params,dest_key=dest,endpoint=ep)
        
        camp_lst = []

        for i in camps_dict:
            camp_df = pd.DataFrame.from_records(i)
            camp_lst.append(camp_df)

        cols = ['campaign_id', 'send_time','title']
        camp_info = pd.concat(camp_lst)
        camp_info.reset_index(drop=True, inplace=True)
        camp_info.columns = cols

        return camp_info

    def get_activity(self,camp_ids = [], count=500,dest='email_activity'):
        """
        ARGS:
                params {dict} = Query parameters and fields passed to the API call
                camp_ids {list} = List of campaign ids returned
                                    from the get_campaign() function OR a Series

        OUTPUT:
                user_activity = Pandas dataframe of user activity organized by
                user email and campaing id
        """
        params={'count':count,
                'fields':'emails.campaign_id,emails.email_address,''emails.activity,emails.activity.action'}
        
        activity = []
        camp_info = self.get_campaign()
        
        for i,c in enumerate(camp_info['campaign_id']):
            ep = 'https://us17.api.mailchimp.com/3.0/reports/'+c+'/email-activity'
            print(ep)
            emails = self.get_json(dest_key='emails',
                         endpoint=ep,
                         params=params)
            
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
    
    def get_members(self,count=500, list_id='190ffb16f9',dest='members'):
        """ Gets member list stats and other information"""
        ep = parse.urljoin(self.api_root,'lists/'+list_id+'/'+dest)
        print(ep)
        params = {'count':count,
                  'fields':'members.id,members.email_address,\
                   members.unique_email_id,members.status,members.stats,members.email_client,members.location'}
        
       
        members = self.get_json(dest_key=dest,params=params,endpoint=ep)
        df = pd.DataFrame(members)
        member_data = pd.concat([df.drop(['location','stats'],axis=1),
                       pd.DataFrame(df['location'].tolist()),
                       pd.DataFrame(df['stats'].tolist())],axis=1)
        return member_data
        
    
    
