import json
import argparse
import sys
import requests
import logging
from requests.exceptions import ConnectionError, ConnectTimeout

def run_loader(load_url, server, port, infile, halt_on_error=False):    
    with open(infile) as jfile: 
        json_input = json.load(jfile)
        post_url = "http://%s:%s/%s" % (server,port,load_url)
        logging.info("Using post url to load %s" % post_url)
        count_200 = 0
        count_201 = 0
        count_other = 0
        count_total = 0
        for x in json_input:
            count_total+=1
            try:
                r = requests.post(post_url,json=x,)                    
                if r.status_code >= 400:
                    logging.error("Error.  %s  Body: %s" % (r,r.content))
                    if halt_on_error:
                        logging.error("Halting. Input that caused the issue: %s" %x)
                        sys.exit(1)
                else:
                    if r.status_code == 200:
                        count_200+=1
                        logging.debug("Resp: %s  Body: %s" % (r,r.content))
                    elif r.status_code == 201:
                        count_201+=1 
                        logging.debug("Resp: %s  Body: %s" % (r,r.content))
                    else: 
                        count_other+=1
                        logging.info("Resp: %s  Body: %s" % (r,r.content))
            except ConnectionError as err:
                logging.error("Connection error, halting %s" % err)
                if halt_on_error:
                    logging.error("Halting. Input that caused the issue: %s" %x)
                    sys.exit(1)
                return
            except:
                logging.error("Unexpected error: %s" % sys.exc_info()[0])
                if halt_on_error:
                    logging.error("Halting. Input that caused the issue: %s" %x)
                    sys.exit(1)
                raise
        logging.info("Finished. Total: %s Count of 200:%s Count of 201:%s Count of other <400:%s" %(count_total,count_200,count_201,count_other))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-f","--file", dest="file", help="Input json file",required=True)
    parser.add_argument("-s","--server", help="Server hostname (default localhost)",default="localhost")
    parser.add_argument("-p","--port", help="Server port (default 30235)",default=30235, type=int)
    parser.add_argument("-e","--endpoint", help="Server endpoint/path (default inspections)",default="inspections")
    parser.add_argument("--halt", help="Halt on error",action="store_true")
    parser.add_argument("-v", "--verbose", help="Show detailed log messages", action="store_true")
    config = parser.parse_args()
    if config.verbose:
        logging.basicConfig(level=logging.DEBUG)
        logging.debug("Logging level set to debug")
        print("verbose")
    else:
        logging.basicConfig(level=logging.INFO)

    run_loader(config.endpoint,config.server,config.port,config.file, config.halt)



