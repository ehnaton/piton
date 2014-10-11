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
    
    CONSUMER_KEY = ''
    CONSUMER_SECRET = ''
    OAUTH_TOKEN = ''
    OAUTH_TOKEN_SECRET = ''
        
    auth = twitter.oauth.OAuth(OAUTH_TOKEN, OAUTH_TOKEN_SECRET,
                               CONSUMER_KEY, CONSUMER_SECRET)
    
    twitter_api = twitter.Twitter(auth=auth)
    return twitter_api

def make_twitter_request(twitter_api_func, max_errors=10, *args, **kw): 
    
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
            
twitter_api = oauth_login()
print twitter_api

def harvest_followers_ids(screen_names=[]):
    for screen_name in screen_names:
        store_friends_followers_ids(twitter_api, screen_name=screen_name, 
                                    friends_limit=0, database=screen_name)

harvest_followers_ids(screen_names=[ 'readdle' ])

print "Done"

def harvest_followers_profiles(screen_names=[]): 
    for screen_name in screen_names:
        followers_ids = load_from_mongo(screen_name, 'followers_ids')
        
        all_ids = [ _id for ids_batch in followers_ids for _id in ids_batch['ids'] ]
        
        store_user_info(twitter_api, user_ids=all_ids, database=screen_name)

harvest_followers_profiles(screen_names=[ 'readdle' ])

print "Done."

readdle_followers_counts = sorted([f['followers_count'] 
                                      for f in load_from_mongo('readdle', 'followers_profiles', 
                                                         projection={'followers_count' : 1, '_id' : 0})])
plt.loglog(readdle_followers_counts)
plt.ylabel("Num Followers")
plt.xlabel("Follower Rank")

bins = [0,5,10,100,200,300,400,500,1000,4000]
plt.hist(readdle_followers_counts[:len(readdle_followers_counts)/100*95], bins=bins)

plt.title("Readdle Followers")
plt.xlabel('Bins (range of popularity for Readdle followers)')
plt.ylabel('Number of followers in bin')

MIN = 10
readdle_suspect_followers = [f 
                                for f in load_from_mongo('readdle', 'followers_profiles', 
                                                          projection={'followers_count' : 1, 'id' : 1, '_id' : 0})
                                if f['followers_count'] < MIN]

print "Readdle has {0} 'suspect' followers for MIN={1}".format(len(readdle_suspect_followers), MIN)

readdle_suspect_followers_counts = sorted([f['followers_count'] 
                                              for f in readdle_suspect_followers], reverse=True)

plt.hist(readdle_suspect_followers_counts)
plt.title("Readdle Suspect Followers")
plt.xlabel('Bins (range of followers)')
plt.ylabel('Number of followers in each bin')

print "{0} of Readdle followers have 0 followers"\
.format(sum([1 for c in readdle_suspect_followers_counts if c < 1]))

print "{0} of Readdle followers have 1 follower"\
.format(sum([1 for c in readdle_suspect_followers_counts if c <= 1]))

print "{0} of Readdle followers have less than 3 followers"\
.format(sum([1 for c in readdle_suspect_followers_counts if c < 3]))

print "{0} of Readdle followers have less than 4 followers"\
.format(sum([1 for c in readdle_suspect_followers_counts if c < 4]))

print "{0} of Readdle followers have less than 5 followers"\
.format(sum([1 for c in readdle_suspect_followers_counts if c < 5]))

harvest_followers_ids(screen_names=[ 'ABBYY_Software' ])
harvest_followers_profiles(screen_names=[ 'ABBYY_Software' ])
print "Done."

abbyy_followers_counts = sorted([f['followers_count'] 
                                      for f in load_from_mongo('ABBYY_Software', 'followers_profiles', 
                                                         projection={'followers_count' : 1, '_id' : 0})])
                                                         
plt.loglog(abbyy_followers_counts)
plt.ylabel("Num Followers")
plt.xlabel("Follower Rank")

bins = [0,5,10,100,200,300,400,500,1000,4000]
plt.hist(abbyy_followers_counts[:len(abbyy_followers_counts)/100*98], bins=bins)

plt.title("ABBYY Followers")
plt.xlabel('Bins (range of popularity for ABBYY followers)')
plt.ylabel('Number of followers in bin')

MIN = 10
abbyy_suspect_followers = [f 
                                for f in load_from_mongo('ABBYY_Software', 'followers_profiles', 
                                                          projection={'followers_count' : 1, 'id' : 1, '_id' : 0})
                                if f['followers_count'] < MIN]

abbyy_suspect_followers_counts = sorted([f['followers_count'] 
                                              for f in abbyy_suspect_followers], reverse=True)

plt.hist(abbyy_suspect_followers_counts)
plt.title("ABBYY Suspect Followers")
plt.xlabel('Bins (range of followers)')
plt.ylabel('Number of followers in each bin')
print "ABBYY has {0} 'suspect' followers for MIN={1}".format(len(abbyy_suspect_followers), MIN)

print "{0} of ABBYY followers have 0 followers"\
.format(sum([1 for c in abbyy_suspect_followers_counts if c < 1]))

print "{0} of ABBYY followers have 1 follower"\
.format(sum([1 for c in abbyy_suspect_followers_counts if c <= 1]))

print "{0} of ABBYY followers have less than 3 followers"\
.format(sum([1 for c in abbyy_suspect_followers_counts if c < 3]))

print "{0} of ABBYY followers have less than 4 followers"\
.format(sum([1 for c in abbyy_suspect_followers_counts if c < 4]))

print "{0} of ABBYY followers have less than 5 followers"\
.format(sum([1 for c in abbyy_suspect_followers_counts if c < 5]))

readdle_followers_ids = set([fid
                 for ids in load_from_mongo('readdle', 'followers_ids', projection={'ids' : 1})
                     for fid in ids['ids']
                 ])

abbyy_followers_ids = set([fid
                 for ids in load_from_mongo('ABBYY_Software', 'followers_ids', projection={'ids' : 1})
                     for fid in ids['ids']
                 ])

# Now, calculate the number of followers in common between each person of interest
# by using set intersections.

readdle_abbyy_common_followers_ids = readdle_followers_ids & abbyy_followers_ids

print "Readdly and ABBYY have {0} followers in common."\
.format(len(readdle_abbyy_common_followers_ids))

readdle_suspect_followers_ids = set([f['id'] for f in readdle_suspect_followers])

print "{0} of Readdle 'suspect' followers are from the set that's in common with ABBYY followers"\
.format(len(readdle_suspect_followers_ids & readdle_abbyy_common_followers_ids))

harvest_followers_ids(screen_names=[ 'thegrizzlylabs' ])
harvest_followers_profiles(screen_names=[ 'thegrizzlylabs' ])
print "Done."
def jaccard(x,y): 
    return 1.0*len(x & y) / len(x | y)

readdle_abbyy_jaccard = jaccard(readdle_followers_ids, abbyy_followers_ids)

print "Readdle and ABBYY Jaccard Index: {0}".format(readdle_abbyy_jaccard)

grizzly_followers_ids = set([fid
                 for ids in load_from_mongo('thegrizzlylabs', 'followers_ids', projection={'ids' : 1})
                     for fid in ids['ids']
                 ])

grizzly_abbyy_jaccard = jaccard(grizzly_followers_ids, abbyy_followers_ids)
print "Grizzly and ABBYY Jaccard Index: {0}".format(grizzly_abbyy_jaccard)

readdle_grizzly_jaccard = jaccard(readdle_followers_ids, grizzly_followers_ids)
print "Readdle and Grizzly Jaccard Index {0}".format(readdle_grizzly_jaccard)

MIN = 10
grizzly_suspect_followers = [f 
                                for f in load_from_mongo('thegrizzlylabs', 'followers_profiles', 
                                                          projection={'followers_count' : 1, 'id' : 1, '_id' : 0})
                                if f['followers_count'] < MIN]

grizzly_suspect_followers_ids = set([f['id'] for f in grizzly_suspect_followers])

readdle_followers_ids_not_suspect = readdle_followers_ids - readdle_suspect_followers_ids

readdle_abby_jaccard_not_suspect = jaccard(readdle_followers_ids_not_suspect, abbyy_followers_ids)
print "Readdle and ABBYY Jaccard Index adjusted for suspect followers: {0}"\
.format(readdle_abby_jaccard_not_suspect)

# Need to define this variable, assuming you've pulled down the data for this account

grizzly_followers_ids = set([fid
                 for ids in load_from_mongo('thegrizzlylabs', 'followers_ids', projection={'ids' : 1})
                     for fid in ids['ids']
                 ])

grizzly_followers_ids_not_suspect = grizzly_followers_ids - grizzly_suspect_followers_ids

grizzly_abbyy_jaccard_not_suspect = jaccard(grizzly_followers_ids_not_suspect, abbyy_followers_ids)
print "Grizzly and ABBYY Jaccard Index adjusted for suspect followers: {0}"\
.format(grizzly_abbyy_jaccard_not_suspect)

readdle_grizzly_jaccard_not_suspect = jaccard(readdle_followers_ids_not_suspect, grizzly_followers_ids)
print "Readdle and Grizzly Jaccard Index adjusted for suspect followers {0}"\
.format(readdle_grizzly_jaccard_not_suspect)

all_common_followers_ids = grizzly_followers_ids & abbyy_followers_ids & grizzly_followers_ids
print len(all_common_followers_ids)
