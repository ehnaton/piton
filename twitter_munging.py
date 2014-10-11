from functools import partial
from sys import maxint
import sys
import time
from urllib2 import URLError
from httplib import BadStatusLine
import json
import twitter # pip install twitter
import pymongo # pip install pymongo


def oauth_login():
    # XXX: Go to http://twitter.com/apps/new to create an app and get values
    # for these credentials that you'll need to provide in place of these
    # empty string values that are defined as placeholders.
    # See https://dev.twitter.com/docs/auth/oauth for more information 
    # on Twitter's OAuth implementation
    
    CONSUMER_KEY = 'QUJGt51huxEji4gA10HRZpCEV'
    CONSUMER_SECRET = 'c3JdUFuFh3mmY4tDCVXNf1wLBjrE425dY3YkONaPTB7C7zEmew'
    OAUTH_TOKEN = '1449820634-gs6ZzPaNOQVk0BYndR5tQdcieCAu409m7dKPyL3'
    OAUTH_TOKEN_SECRET = 'PrM7KkhS6nQN1mY0PSWb19Gi6mV2UqW7ohGICuRc6vz3k'
        
    auth = twitter.oauth.OAuth(OAUTH_TOKEN, OAUTH_TOKEN_SECRET,
                               CONSUMER_KEY, CONSUMER_SECRET)
    
    twitter_api = twitter.Twitter(auth=auth)
    return twitter_api

def make_twitter_request(twitter_api_func, max_errors=10, *args, **kw): 
    
    # A nested helper function that handles common HTTPErrors. Return an updated value 
    # for wait_period if the problem is a 500 level error. Block until the rate limit 
    # is reset if a rate limiting issue (429 error). Returns None for 401 and 404 errors
    # which requires special handling by the caller.
    def handle_twitter_http_error(e, wait_period=2, sleep_when_rate_limited=True):
    
        if wait_period > 3600: # Seconds
            print >> sys.stderr, 'Too many retries. Quitting.'
            raise e
    
        # See https://dev.twitter.com/docs/error-codes-responses for common codes
    
        if e.e.code == 401:
            print >> sys.stderr, 'Encountered 401 Error (Not Authorized)'
            return None
        elif e.e.code == 404:
            print >> sys.stderr, 'Encountered 404 Error (Not Found)'
            return None
        elif e.e.code == 429: 
            print >> sys.stderr, 'Encountered 429 Error (Rate Limit Exceeded)'
            if sleep_when_rate_limited:
                print >> sys.stderr, "Sleeping for 15 minutes, and then I'll try again...ZzZ..."
                sys.stderr.flush()
                time.sleep(60*15 + 5)
                print >> sys.stderr, '...ZzZ...Awake now and trying again.'
                return 2
            else:
                raise e # Allow user to handle the rate limiting issue however they'd like 
        elif e.e.code in (500, 502, 503, 504):
            print >> sys.stderr, 'Encountered %i Error. Will retry in %i seconds' % (e.e.code,
                    wait_period)
            time.sleep(wait_period)
            wait_period *= 1.5
            return wait_period
        else:
            raise e

    # End of nested helper function
    
    wait_period = 2 
    error_count = 0 

    while True:
        try:
            return twitter_api_func(*args, **kw)
        except twitter.api.TwitterHTTPError, e:
            error_count = 0 
            wait_period = handle_twitter_http_error(e, wait_period)
            if wait_period is None:
                return
        except URLError, e:
            error_count += 1
            print >> sys.stderr, "URLError encountered. Continuing."
            if error_count > max_errors:
                print >> sys.stderr, "Too many consecutive errors...bailing out."
                raise
        except BadStatusLine, e:
            error_count += 1
            print >> sys.stderr, "BadStatusLine encountered. Continuing."
            if error_count > max_errors:
                print >> sys.stderr, "Too many consecutive errors...bailing out."
                raise
                
def store_friends_followers_ids(twitter_api, screen_name=None, user_id=None,
                              friends_limit=maxint, followers_limit=maxint, database=None):
    
    # Must have either screen_name or user_id (logical xor)
    assert (screen_name != None) != (user_id != None), "Must have screen_name or user_id, but not both"
    
    # See https://dev.twitter.com/docs/api/1.1/get/friends/ids  and
    # See https://dev.twitter.com/docs/api/1.1/get/followers/ids for details on API parameters
    
    get_friends_ids = partial(make_twitter_request, twitter_api.friends.ids, count=5000)
    get_followers_ids = partial(make_twitter_request, twitter_api.followers.ids, count=5000)
    
    for twitter_api_func, limit, label in [
                                 [get_friends_ids, friends_limit, "friends"], 
                                 [get_followers_ids, followers_limit, "followers"]
                             ]:
        
        if limit == 0: continue
        
        total_ids = 0
        cursor = -1
        while cursor != 0:
        
            # Use make_twitter_request via the partially bound callable...
            if screen_name: 
                response = twitter_api_func(screen_name=screen_name, cursor=cursor)
            else: # user_id
                response = twitter_api_func(user_id=user_id, cursor=cursor)

            if response is not None:
                ids = response['ids']
                total_ids += len(ids)
                save_to_mongo({"ids" : [_id for _id in ids ]}, database, label + "_ids")
                cursor = response['next_cursor']
        
            print >> sys.stderr, 'Fetched {0} total {1} ids for {2}'.format(total_ids, label, (user_id or screen_name))
            sys.stderr.flush()
        
            # Consider storing the ids to disk during each iteration to provide an 
            # an additional layer of protection from exceptional circumstances
        
            if len(ids) >= limit or response is None:
                break
                print >> sys.stderr, 'Last cursor', cursor
                print >> sts.stderr, 'Last response', response

def save_to_mongo(data, mongo_db, mongo_db_coll, auth=None, **mongo_conn_kw):
    
    # Connects to the MongoDB server running on 
    # localhost:27017 by default
    
    client = pymongo.MongoClient(**mongo_conn_kw)
    
    # Get a reference to a particular database
    
    db = client[mongo_db]
    if auth:
        db.authenticate(auth[0], auth[1])
        
    # Reference a particular collection on the database
    
    coll = db[mongo_db_coll]
    
    # Perform a bulk insert and  return the ids
    
    return coll.insert(data)

def load_from_mongo(mongo_db, mongo_db_coll, return_cursor=False,
                    criteria=None, projection=None, auth=None, **mongo_conn_kw):
    
    # Optionally, use criteria and projection to limit the data that is 
    # returned as documented in 
    # http://docs.mongodb.org/manual/reference/method/db.collection.find/
    
    # Consider leveraging MongoDB's aggregations framework for more 
    # sophisticated queries.
    
    client = pymongo.MongoClient(**mongo_conn_kw)
    db = client[mongo_db]
    
    if auth:
        db.authenticate(auth[0], auth[1])

    coll = db[mongo_db_coll]
    
    if criteria is None:
        criteria = {}
    
    if projection is None:
        cursor = coll.find(criteria)
    else:
        cursor = coll.find(criteria, projection)

    # Returning a cursor is recommended for large amounts of data
    
    if return_cursor:
        return cursor
    else:
        return [ item for item in cursor ]
    
def store_user_info(twitter_api, screen_names=None, user_ids=None, database=None):
   
    # Must have either screen_name or user_id (logical xor)
    assert (screen_names != None) != (user_ids != None), "Must have screen_names or user_ids, but not both"
    
    items = screen_names or user_ids
    
    while len(items) > 0:
        if len(items)/100*100 % 1000 == 0:
            print >> sys.stderr, len(items), "remaining"
            
        # Process 100 items at a time per the API specifications for /users/lookup. See
        # https://dev.twitter.com/docs/api/1.1/get/users/lookup for details
        
        items_str = ','.join([str(item).replace("\\", "\\\\") for item in items[:100]])
        items = items[100:]

        if screen_names:
            response = make_twitter_request(twitter_api.users.lookup, screen_name=items_str)
        else: # user_ids
            response = make_twitter_request(twitter_api.users.lookup, user_id=items_str)
    
        for profile in response:            
            save_to_mongo(profile, database, 'followers_profiles')
            
# Go ahead and instantiate an instance of the Twitter API for common use
# throughout the rest of this notebook.

twitter_api = oauth_login()
print twitter_api
