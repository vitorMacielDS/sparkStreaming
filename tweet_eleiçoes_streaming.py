# Databricks notebook source
import os, uuid, sys
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings
import tweepy
import json
from tweepy import OAuthHandler
from tweepy import API
from tweepy import Cursor
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


consumer_key = "LGtwUGUZwKQv6UBGZnjWKmbDx"
consumer_secret = "NTyH0TXdVrWaQF9cJDKIcRCj5XS7DTiSpCOLiaMbmnu4EQztmm"
access_token = "3817339659-lovwfHUIKreHjqbfMQbYzW6RaU6OT6yNoFeDTPd"
access_token_secret = "HjEWhNRzfkszQWSRhCNxL6bp9NmkCFQyGPDULAklQDtUK"
storage_account_name = "vitordata"
storage_account_key = "etbTL7GxZt/jWxpKsD9Iw/AITADlEje7Z98AvABYqtdCQT+XuR08jac2da0M75dm6DQlbId2SO63doEzvZ4tfg=="


auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

auth_api = tweepy.API(auth)
elections_twitter = tweepy.Cursor(auth_api.search, q="Bolsonaro",rpp=100, result_type="recent", include_entities=True, lang="pt-br").items(100) 
    
final_file=[]
for tweet in elections_twitter:
  
  final_file += json.dumps(tweet._json)
  print(tweet)

with open('tweets.json', 'w') as json_file:
  json.dump(final_file, json_file)

def initialize_storage_account(storage_account_name, storage_account_key):

    try:
        global service_client

        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)

    except Exception as e:
        print(elections_twitter)
        
def upload_file_to_directory_bulk():
    try:

        file_system_client = service_client.get_file_system_client(file_system="vitor-rawdata")

        directory_client = file_system_client.get_directory_client("twitter_2021")

        file_client = directory_client.get_file_client("final_file.json")

        local_file = final_file

        file_contents = local_file

        file_client.upload_data(file_contents, overwrite=True)

    except Exception as e:
      print()

initialize_storage_account('vitordata', 'etbTL7GxZt/jWxpKsD9Iw/AITADlEje7Z98AvABYqtdCQT+XuR08jac2da0M75dm6DQlbId2SO63doEzvZ4tfg==')
upload_file_to_directory_bulk()


