from bottle import Bottle, post, get, HTTPResponse, request, response
import argparse
import os
import sys
import psycopg2 as pg
import logging
import string
from db import DB
import json

logging.basicConfig(level=logging.INFO)
app = Bottle()

txnsize_global = 1
load_count = 0


@app.get("/hello")
def hello():
    return "Hello, World!"


@app.get("/restaurants/<restaurant_id:int>")
def find_restaurant(restaurant_id):
    """
    Returns a restaurant and all of its associated inspections.
    """

    db = DB(app.db_connection)

    status, restaurant = db.find_restaurant(restaurant_id)
    inspections = db.find_inspections(restaurant_id)[1]
    response.status = status

    # format restaurant data into pretty JSON
    data = json.dumps({'restaurant': restaurant,
                       'inspections': inspections},
                       sort_keys=False, indent=4)

    return data


@app.get("/restaurants/by-inspection/<inspection_id>")
def find_restaurant_by_inspection_id(inspection_id):
    """
    Returns a restaurant associated with a given inspection.
    """

    db = DB(app.db_connection)

    # get the inspection record
    status, inspection = db.find_inspection(inspection_id)

    # throw an error if the inspection doesn't exist
    response.status = status
    if status >= 400:
        return None

    # grab the restaurant identifier from the inspection record and find 
    # get the associated restaurant record
    restaurant_id = inspection['restaurant_id']
    status, restaurant = db.find_restaurant(restaurant_id)

    # throw an error if the restaurant doesn't exist
    response.status = status
    if status >= 400:
        return None

    # format the restaurant record and send back
    data = json.dumps(restaurant, sort_keys=False, indent=4)

    return data


# type check zip, state, 
@app.post("/inspections")
def load_inspection():
    """
    Loads a new inspection (and possibly a new restaurant) into the database.
    """

    global load_count
    global txnsize_global

    db = DB(app.db_connection)

    # load the json data into a list of dictionaries 
    # or a dict in the case of a single record
    record = request.json

    # check validity of inputs for zipcode and state 
    if record['zip'] and not record['zip'].isnumeric():
        if load_count > 0:
            app.db_connection.rollback()
        response.status = 400
        return None

    if record['state'] and not record['state'].isalpha():
        if load_count > 0:
            app.db_connection.rollback()
        response.status = 400
        return None

    # if given a clean status use it, otherwise set to None
    try:
        clean = record['clean']
    except KeyError:
        clean = False

    # parse inspection data from restaurant data
    # load each inspection into the database
    inspection = {
            'id' : record['inspection_id'],
            'risk' : record['risk'],
            'inspection_date' : record['date'],
            'inspection_type' : record['inspection_type'],
            'results' : record['results'],
            'violations' : record['violations'],
    }
    restaurant = {
            'name' : record['name'],
            'facility_type' : record['facility_type'],
            'address' : record['address'],
            'city' : record['city'],
            'state' : record['state'],
            'zip' : record['zip'],
            'location' : record['location'],
            'clean' : clean
    }

    # set response status and send back dictionary with restaurant id
    # respond with url for restaurant in header
    status, rest_id = db.add_inspection_for_restaurant(inspection, restaurant)
    response.status = status

    if status >= 400:
        load_count = 0
        return None
    else:
        load_count += 1
        if load_count >= txnsize_global:
            app.db_connection.commit()
            load_count = 0
        
    url_path_rest = 'http://localhost:30235/restaurants/'
    response.add_header('Location', url_path_rest + str(rest_id))

    return {'restaurant_id' : rest_id}


@app.get("/txn/<txnsize:int>")
def set_transaction_size(txnsize):
    '''
    This endpoint allows you to specify the number (transaction size) of post inspection
    requeststhat should be batched together for a transaction commit. 
    '''

    global load_count
    global txnsize_global

    if load_count > 0:
        app.db_connection.commit()
        load_count = 0

    txnsize_global = txnsize
    response.status = 200

    return None


@app.get("/abort")
def abort_txn():
    '''
    This endpoint aborts/rollback any active transaction.
    '''

    global load_count

    logging.info("Aborting active transactions")
    app.db_connection.rollback()
    load_count = 0
    response.status = 200

    return None
 

@app.get("/bulkload/<file_name:path>")
def bulk_load(file_name):
    '''
    Get the file name of a local csv containing inspection records, 
    load it into a temp table,
    and transfer the records from temp table to their correct tables and schema
    '''

    base_dir = "../data"
    file_path = os.path.join(base_dir,file_name)

    db = DB(app.db_connection)

    try:
        with open(file_path, 'r') as data:
            response.status = db.bulk_loading(data)
    except FileNotFoundError:
        response.status = 404
        return None
    except Exception:
        print(f'Error reading file {file_name}')
        response.status = 400
        return None

    return None


@app.get("/reset")
def reset_db():
    '''
    This endpoint simply resets the state of the database, by truncating all tables 
    in the database. If there are any active transactions they must be aborted first.
    '''

    abort_txn()

    logging.info("Reseting DB")
    db = DB(app.db_connection)
    status = db.reset_db()

    response.status = status

    return None
    
    
@app.get("/count")
def count_insp():
    '''
    This endpoint counts the number of records in the ri_inspections 
    table and returns a simple json object {"count" : N } where N is the number
    of records in the table.
    '''

    logging.info("Counting Inspections")
    db = DB(app.db_connection)

    status, N = db.count_all_insp()
    response.status = status

    return {'count': N}
    

def ngrams(tweet, n):
    #single_word = tweet.translate(str.maketrans('', '', string.punctuation)).split()
    single_word = tweet.translate(str.maketrans('', '', ".")).split()
    
    output = []
    for i in range(len(single_word)-n+1):
        output.append(' '.join(single_word[i:i+n]).upper())
    return output


@app.post("/tweet")
def tweet():
    '''
    Receive JSON of a tweet. Extract text and location data. If either matches
    with a restaurant in the DB, we insert the tweet into the ri_tweetmatch
    table with the matching restaurant ids. We return the restaurant ids to 
    the user.
    '''
    logging.info("Checking Tweet")
    data = request.json

    try:
        if data['lat'] and data['long']:
            lat = float(data['lat'])
            lon = float(data['long'])
        else:
            lat = None
            lon = None

        text = data['text'].upper()
        ngram = []
        for n in range(1,5):
            ngram += ngrams(text, n)

        tweet_data = {
            'tkey': data['key'],
            'ngram': ngram,
            'long': lon,
            'lat': lat
            }

    except KeyError:
        response.status = 400
        logging.error('Error parsing JSON from client')
        return None

    db = DB(app.db_connection)
    status, restaurant_ids = db.match_tweet(tweet_data)
    response.status = status

    if status >= 400:
        logging.error('Error inserting/matching tweet')
        return None

    data = json.dumps({'match': restaurant_ids}, sort_keys=False, indent=4)

    return data


@app.get("/buildidx")
def build_indexes():
    logging.info("Building indexes")
    
    db = DB(app.db_connection)
    status = db.add_restaurants_index()

    response.status = status

    return None


@app.get("/tweets/<inspection_id>")
def find_tweet_keys_by_inspection_id(inspection_id):
    logging.info("Finding tweet keys by inspection ID")

    db = DB(app.db_connection)
    status, tkeys = db.get_tweets_by_insp(inspection_id)

    response.status = status
    data = json.dumps({'tkeys': tkeys}, sort_keys=False, indent=4)

    return data


@app.get("/clean")
def clean_restaurants():
    logging.info("Cleaning Restaurants")
    
    db = DB(app.db_connection)

    if app.scaling:
        status = db.find_and_update_linked_restaurants_fast()
    else:
        status = db.find_and_update_linked_restaurants()

    response.status = status

    return None

    
@app.get("/restaurants/all-by-inspection/<inspection_id>")
def find_all_restaurants_by_inspection_id(inspection_id):
    logging.info("Get All Restaurants")
    
    db = DB(app.db_connection)
    status, restaurants = db.find_all_restaurants(inspection_id)

    response.status = status
    data = json.dumps(restaurants, sort_keys=False, indent=4)

    return data
 

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c","--config",
        help="The path to the .conf configuration file.",
        default="server.conf"
    )
    parser.add_argument(
        "--host",
        help="Server hostname (default localhost)",
        default="localhost"
    )
    parser.add_argument(
        "-p","--port",
        help="Server port (default 30235)",
        default=30235,
        type=int
    )

    parser.add_argument(
        "-s","--scaling",
        help="Enable large scale cleaning",
        default=False,
        action="store_true"
    )


    args = parser.parse_args()
    if not os.path.isfile(args.config):
        logging.error("The file \"{}\" does not exist!".format(args.config))
        sys.exit(1)

    app.config.load_config(args.config)
    app.scaling=False
    try:
        app.db_connection = pg.connect(
            dbname = app.config['db.dbname'],
            user = app.config['db.user'],
            password = app.config.get('db.password'),
            host = app.config['db.host'],
            port = app.config['db.port']
        )
    except KeyError as e:
        logging.error("Is your configuration file ({})".format(args.config) +
                      " missing options?")
        raise


    try:
        if args.scaling:
            app.scaling = True
        logging.info("Starting Inspection Service. App Scaling= %s" % (app.scaling))
        app.run(host=args.host, port=args.port)
    finally:
        app.db_connection.close()

