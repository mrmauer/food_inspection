# Imports
import argparse
import itertools
import json
import sys
import logging
import traceback
from timeit import default_timer as timer
from hdrh import histogram
import requests
from requests.exceptions import ConnectionError, ConnectTimeout

def get_stat_string(hist):
    if hist.get_total_count() == 0:
        d = {50:0, 95:0, 99:0, 100:0}
    else:
        d = hist.get_percentile_to_value_dict([50, 95, 99, 100])

    return "Latency Perecentiles(ms) - 50th:%4.2f, 95th:%4.2f, 99th:%4.2f, 100th:%4.2f - Count:%s" % (d[50], d[95], d[99], d[100], hist.get_total_count())


def load_file(jsonfile, endpoint, halt_on_error, id_attr=None, ids_to_keep=[], limit=None):
    with open(jsonfile) as f:
        json_input = json.load(f)
        counts = {200: 0,
                  201: 0,
                  'other': 0,
                  'total': 0}
        responses_to_keep = {}
        hist = histogram.HdrHistogram(1, 1000 * 60 * 60, 2)
        count = 0
        for x in json_input:
            counts['total'] += 1
            try:
                start = timer()
                r = requests.post(endpoint, json=x)
                end = timer()
                if r.status_code >= 400:
                    logging.error("Error.  %s  Body: %s" % (r, r.content))
                    if halt_on_error:
                        logging.error("Halting. Input that caused the issue: %s" %x)
                        sys.exit(1)
                else:
                    if r.status_code == 200 or r.status_code == 201:
                        counts[r.status_code] += 1
                        hist.record_value((end - start) * 1000)
                        logging.debug("Resp: %s  Body: %s" % (r,r.content))
                    else:
                        counts['other'] += 1
                        logging.info("Resp: %s  Body: %s" % (r, r.content))
                    # Check if we should save response
                    if id_attr and x[id_attr] in ids_to_keep:
                        responses_to_keep[x[id_attr]] = r.json()
            except ConnectionError as err:
                logging.error("Connection error, halting %s" % err)
                if halt_on_error:
                    logging.error("Halting. Input that caused the issue: %s" %x)
                    sys.exit(1)
                return
            except:
                logging.error("Unexpected error: %s" % sys.exc_info()[0])
                traceback.print_exc()
                if halt_on_error:
                    logging.error("Halting. Input that caused the issue: %s" %x)
                    sys.exit(1)
                raise
            count+=1
            if limit and count >= limit:
                logging.info("Breaking early due to limit %s " % limit)
                break
    return counts, hist, responses_to_keep

def build_idx(server, port):
    idx_url = 'http://{}:{}/buildidx'.format(server, port) #TODO
    start = timer()
    r = requests.get(idx_url)
    idx_time = (timer() - start) * 1000
    return r.status_code, idx_time

# MAIN FLOW
def run_loader(server, port, insp_file, tweet_file, index_timing, load_type, halt_on_error=False, limit=None, clean=False):
    logging.info("Calling loader")
    # Reset db
    reset_url = 'http://{}:{}/reset'.format(server, port)
    reset_r = requests.get(reset_url)
    if reset_r.status_code != 200:
        logging.info('Fatal error: could not reset database')
        sys.exit(1)

    # Index pre-insert
    idx_time = 0
    if index_timing == 'pre':
        idx_status, idx_time = build_idx(server, port)
        if not idx_status == 200:
            logging.info('Unable to build index, skipping current config')
            return

    # Inspection loading
    if load_type == 'bulk':
        insp_endpoint = "http://{}:{}/bulkload/{}".format(server, port, insp_file)
        start_time = timer()
        insp_r = requests.get(insp_endpoint)
        insp_time = timer() - start_time
        if insp_r.status_code != 200:
            logging.info('Fatal error: bulk load unsuccessful, skipping current config')
            return
    else:
        # Set transaction size
        txn_endpoint = "http://{}:{}/txn/{}".format(server, port, load_type)
        txn_r = requests.get(txn_endpoint)
        if txn_r.status_code != 200:
            logging.info('Fatal error: could not set transaction size before loading inspections')
            sys.exit(1)
        # Load inspections
        insp_endpoint = "http://{}:{}/inspections".format(server, port)
        insp_counts, insp_hist, insp_responses = load_file(insp_file, insp_endpoint, halt_on_error, "inspection_id", ["2370195","1"], limit)
        insp_time = insp_hist.get_mean_value() * insp_hist.get_total_count()
        logging.info(insp_responses)
        logging.info('Inspection total load time: {}'.format(insp_time))
        logging.info(get_stat_string(insp_hist))
        logging.info("Total: %s Count of 200:%s Count of 201:%s Count of other <400:%s" %(insp_counts['total'], insp_counts[200], insp_counts[201], insp_counts['other']))

    # Index post-insert
    if index_timing == 'post':
        idx_status, idx_time = build_idx(server, port)
        if not idx_status == 200:
            logging.info('Unable to build index, skipping current config')
            return

    # Tweet loading
    if tweet_file:
        tweet_txn_endpoint = "http://{}:{}/txn/1".format(server, port)
        tweet_txn_r = requests.get(tweet_txn_endpoint)
        if tweet_txn_r.status_code != 200:
            logging.info('Fatal error: could not set transaction size before loading tweets')
            sys.exit(1)
        tweet_endpoint = "http://{}:{}/tweet".format(server, port)
        tweet_counts, tweet_hist, tweet_responses = load_file(tweet_file, tweet_endpoint, halt_on_error)
        tweet_time = insp_hist.get_mean_value() * insp_hist.get_total_count()
        logging.info('Tweet load time: {}'.format(tweet_time))
        logging.info(get_stat_string(tweet_hist))
        logging.info("Total: %s Count of 200:%s Count of 201:%s Count of other <400:%s" %(tweet_counts['total'], tweet_counts[200], tweet_counts[201], tweet_counts['other']))
    else:
        logging.info("Skipping Tweets")
        tweet_time = 0

    # Retrieve views?

    # Clean db
    clean_time=0
    if clean:
        start_time = timer()
        clean_url = 'http://{}:{}/clean'.format(server, port)
        clean_r = requests.get(clean_url)
        if clean_r.status_code != 200:
            logging.info('Fatal error: could not clean database')
            sys.exit(1)
        else:
            clean_time = timer() - start_time  
            logging.info('Cleaning time: {}'.format(clean_time))


    # Interact w/ leaderboard?

    total_time = idx_time + insp_time + tweet_time+clean_time
    logging.info('Total time: {}'.format(total_time))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--inspfile", dest="insp_file", help="Input JSON/CSV of inspections", required=True)
    parser.add_argument("-t", "--tweetjson", dest="tweet_file", help="Input JSON of tweets")
    parser.add_argument("-s", "--server", help="Server hostname (default localhost)", default="localhost")
    parser.add_argument("-p", "--port", help="Server port (default 30235)", default=30235, type=int)
    parser.add_argument("--halt", help="Halt on error", action="store_true")
    parser.add_argument('--load', help='Must be bulk, 1, 10, 100, or 1000. How to load records, either bulk (copy) or batching load statements into transactions (eg 10 records batched)', choices=['bulk', '1', '10', '100', '1000'], dest='load_type', default=1)
    parser.add_argument('--index', dest="index_timing", help='Must be pre, post, or never. When to build index(es).', choices=['pre', 'post', 'never'], default='never')
    parser.add_argument("-v", "--verbose", help="Show detailed log messages", action="store_true")
    parser.add_argument("-l", "--limit", help="Limit records for non-bulk loader", default=None, type=int)
    parser.add_argument("--clean", help="Invoke the cleaning script after loading records and tweets", action="store_true")


    config = parser.parse_args()
    #LOG LEVEL
    if config.verbose:
        logging.basicConfig(level=logging.DEBUG)
        logging.debug("Logging level set to debug")
        print("verbose")
    else:
        logging.basicConfig(level=logging.INFO)

    if config.load_type != 'bulk':
        config.load_type = int(config.load_type)
    if not (config.insp_file.endswith('.json') and isinstance(config.load_type, int)) and\
    not (config.insp_file.endswith('.csv') and config.load_type == 'bulk'):
        logging.info("Error: Inspection file type doesn't match load method")
        sys.exit(1)

    run_loader(config.server, config.port, config.insp_file, config.tweet_file,
               config.index_timing, config.load_type, config.halt, config.limit, config.clean)
    