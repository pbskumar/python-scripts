#!/usr/bin/env python
# encoding: utf-8

# to-do
# hash-tag tweet stream


# ############################################################################################
#
#   Extracts tweets from Twitter and inserts them into MongoDB
#
# 	Features:
# 	1. Extracts tweets form a given User's Twitter timeline
# 	2. Extracts tweets of multiple Twitter accounts in parallel
#   3. Updates existing Database of user tweets with all tweets which have been added since
#       the last retrieval/update.
#
# 	Packages Required:
# 	1. Tweepy		(http://www.tweepy.org/)
# 	2. PyMongo		(https://api.mongodb.org/python/current/installation.html)
#
#
# 	Limitation:
# 	Twitter imposes limit on the number of API requests in rate limit window.
# 	Duration of rate limit window is 15 minutes
# 	Only a maximum of ~3200 recent tweets can be extracted from a user time-line
# 	Ref:	1. https://dev.twitter.com/rest/public/rate-limiting
# 			2. https://dev.twitter.com/rest/reference/get/statuses/user_timeline
#
#   Author: Brahmendra Sravan Kumar Patibandla
# 	Created on: 05-APR-2016
# ############################################################################################


import threading
import time

import tweepy
from pymongo import MongoClient
import pymongo
from pymongo.errors import NetworkTimeout, ConnectionFailure


def authenticate_tweepy(consumer_key, consumer_secret_key, access_token, access_secret_token):
    """
    Authenticates the Tweepy API
    :rtype: object
    :param consumer_key: Consumer Key (Available in Twitter Application page)
    :param consumer_secret_key: Secret Consumer Key(Available in Twitter Application page)
    :param access_token: Access Token (Available in Twitter Application page)
    :param access_secret_token: Secret Comsumer Token(Available in Twitter Application page)
    :return: API handle for Tweepy
    """
    print("Authenticating Tweepy")
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret_key)
    auth.set_access_token(access_token, access_secret_token)
    api = tweepy.API(auth)
    return api


class MongoServer:
    """
    The Class handles the operations of MongoDB.
    Client Connection are auto-reset. Ex: termination by the server due to inactivity etc.
    """

    def __init__(self, connection_string):
        """
        MongoServer class is initialized with the given connection string
        A mongo client reference is created and stored as the attribute
        :param connection_string: Mongo server connection string
        """
        print("Establishing MongoDB connection")
        self.connection_string = connection_string
        self.mongo_client = MongoClient(self.connection_string)

    def close_client(self):
        """
        Method closes existing Mongo client reference stored in self.mongo_client attribute
        """
        print("Closing MongoDB connection")
        self.mongo_client.close()

    def reset_client(self):
        """
        Method resets the client connection.
        Closes the existing client reference and initiates a new connection.
        """
        print("Resetting MongoDB Connection")
        self.close_client()
        self.mongo_client = MongoClient(self.connection_string)

    def write_doc_to_collection(self, db_name, collection_name, json_data):
        """
        Inserts input JSON document into collection (collection_name) of database (db_name)
        :param db_name: name of the database
        :param collection_name: name of the collection
        :param json_data: JSON document to be inserted
        """
        mongo_db = self.mongo_client[db_name]
        mongo_col = mongo_db[collection_name]
        # Tries inserting document into the collection.
        # If server connection is terminated, the connection is reset and tries to insert again
        try:
            mongo_col.insert_one(json_data)
        except NetworkTimeout:
            self.reset_client()
            self.write_doc_to_collection(db_name, collection_name, json_data)
        except ConnectionFailure:
            self.reset_client()
            self.write_doc_to_collection(db_name, collection_name, json_data)

    def find_docs(self, db_name, collection_name, doc_filter=None, doc_projection=None, doc_sort=None, doc_limit=0):
        """
        Queries the database and returns matching documents
        :param db_name: Name of the database
        :param collection_name: Name of the collection
        :param doc_filter: Filter document for search, in JSON format
        :param doc_projection: Projection document for projection, in JSON format
        :param doc_sort: List of (key, direction) tuples for sorting the results
        :param doc_limit: Limits the number of resulting documents to the given number
        :return: Cursor of matching documents
        """
        mongo_db = self.mongo_client[db_name]
        mongo_col = mongo_db[collection_name]
        return mongo_col.find(filter=doc_filter, projection=doc_projection, sort=doc_sort, limit=doc_limit)


def retrieve_user_tweet(cursor, api, user_id, since_tweet_id=None):
    """
    This function is a generator which yields one tweet at a time from the cursor.
    If RateLimitError/TweepError is returned from the cursor, the generator sleeps for 15 minutes and restarts fetching tweets.
    :param cursor: Cursor returned by tweepy Cursor of user_timeline
    :param api: tweepy API handle
    :param user_id: Twitter User ID of target account
    :param since_tweet_id: ID of the latest tweet in the previous update
        Used to retrieve tweets posted after the tweet with given tweet ID.
    :return: yields one tweet at a time
    """
    tweet = ''
    while True:
        try:
            tweet = cursor.next()
            yield tweet
        except tweepy.RateLimitError:
            print("Limit Reached. Sleeping")
            # set max_id for cursor
            if tweet != '':
                max_tweet_id = tweet._json["id"]
                cursor = tweepy.Cursor(api.user_timeline, id=user_id, max_id=max_tweet_id,
                                       since_id=since_tweet_id).items()
            time.sleep(15 * 60)
        except tweepy.TweepError:
            print("Tweepy Error. Rate Limit Reached Sleeping")
            # set max_id for cursor
            # max_id extracts tweets older than the given max_id
            if tweet != '':
                max_tweet_id = tweet._json["id"]
                cursor = tweepy.Cursor(api.user_timeline, id=user_id, max_id=max_tweet_id,
                                       since_id=since_tweet_id).items()
            time.sleep(15 * 60)
        except StopIteration:
            print("All tweets of " + user_id + " retrieved.")
            return


def get_user_tweets(user_id, api, db_reference, db_conf_list):
    """
    Retrieves all tweets of a given user.
    If some tweets already exist in database, latest tweets are retrieved and stored in Database
    :param user_id: Twitter User ID of the target account
    :param api: Tweepy API handle
    :param db_reference: Reference of MongoServer
    :param db_conf_list: List of target database name and collection name.
    """

    latest_tweet_id = None
    doc_filter = {"user.screen_name": user_id}
    doc_projection = {"_id": 0, "id": 1}
    doc_sort = [("id", pymongo.DESCENDING)]
    doc_limit = 1
    latest_tweet_doc = db_reference.find_docs(db_conf_list[0], db_conf_list[1], doc_filter=doc_filter,
                                              doc_projection=doc_projection, doc_sort=doc_sort, doc_limit=doc_limit)

    for doc in latest_tweet_doc:
        latest_tweet_id = doc["id"]

    if latest_tweet_id:
        print("Updating tweets of " + user_id)
    else:
        print("Extracting tweets from " + user_id + "'s time-line")

    for tweet in retrieve_user_tweet(tweepy.Cursor(api.user_timeline, id=user_id, since_id=latest_tweet_id).items(),
                                     api, user_id, latest_tweet_doc):
        tweet_json = tweet._json
        db_reference.write_doc_to_collection(db_conf_list[0], db_conf_list[1], tweet_json)


class DataRetrievalThread(threading.Thread):
    """
    Thread class for retrieving tweets of multiple accounts in parallel
    """

    def __init__(self, api, db_reference, db_conf_list, thread_id, retrieval_function):
        """
        Initializes the DataRetrievalThread class
        :param api: Tweepy API Handle
        :param db_reference: MongoServer reference
        :param db_conf_list: List of target database name and collection name.
        :param thread_id: ID of the thread for identification
        :param retrieval_function: The function which has to be executed in the thread
        """
        threading.Thread.__init__(self)
        self.api = api
        self.db_reference = db_reference
        self.db_conf_list = db_conf_list
        self.thread_id = thread_id
        self.retrieval_function = retrieval_function

    def run(self):
        """
        Starts execution of the thread
        """
        print("Starting " + self.name + " " + self.thread_id)
        self.retrieval_function(self.thread_id, self.api, self.db_reference, self.db_conf_list)
        print("Exiting " + self.name)


if __name__ == '__main__':
    # Authentication Keys from Twitter App website
    twitter_consumer_key = ''
    twitter_consumer_secret_key = ''
    twitter_access_token = ''
    twitter_access_secret_token = ''

    # Creating API reference
    tweepy_api = authenticate_tweepy(twitter_consumer_key, twitter_consumer_secret_key, twitter_access_token,
                                     twitter_access_secret_token)

    # Connection string for MongoDB server access
    mongo_connection_string = 'localhost:27017'

    db = MongoServer(mongo_connection_string)
    database_name = 'tweetDB'

  # Use one of the following commented blocks to extract/update tweets
  
  # # Extraction/Updation of tweets from a Twitter Account
  # # Creates a new collection for each User ID and retrieves all available tweets
  # # If some tweeets of an account already exist in the given collection, 
  # get_user_tweets() fetches all Tweets which have been added since the last time the user timeline was processed.
  
  # target_username = "abc"
  # get_user_tweets(target_username, tweepy_api, db, [database_name, target_username])

  
  # # Extraction/Updation of tweets from multiple Twitter time-lines. 
  # # Creates a new collection for each User ID and retrieves all available tweets
  # # If some tweeets of an account already exist in the given collection, 
  # get_user_tweets() fetches all Tweets which have been added since the last time the user timeline was processed.
  
  # candidate_usernames = ['abc', 'def', 'ghi', 'pqr', 'xyz'] # List of Twitter usernames
  # thread_list = [None] * len(candidate_usernames)
  # for index, username in enumerate(candidate_usernames):
  #     thread_list[index] = DataRetrievalThread(tweepy_api, db, [database_name, username], username,
  #                                               get_user_tweets)
  #     thread_list[index].start()

  db.close_connection()
