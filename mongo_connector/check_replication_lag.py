"""
Mongo check latest oplog for given replica sets and
output the difference (lag) in timestamp
"""
# ------- core python imports
import traceback
import time
import argparse
import logging
# ------- core python imports
# ------- package imports
from pymongo.mongo_replica_set_client import MongoReplicaSetClient
from pymongo import MongoClient
from pymongo.read_preferences import ReadPreference
from statsd import StatsClient

# ------- package imports
# ------- elastica imports
# ------- elastica imports
# ------- globals

PARSER = argparse.ArgumentParser(prog='python check_replication_lag.py')
PARSER.add_argument("-s", "--source", dest="source_host", required=True,
                    help="source host to check oplog", metavar="replicaset")
PARSER.add_argument("-t", "--target", dest="target_host", required=True,
                    help="target host to check oplog", metavar="replicaset")
PARSER.add_argument("-r", "--replica", dest="replica_set", required=True,
                    help="replica set name", metavar="replicaset")
PARSER.add_argument("-u", "--user", dest="user", required=True,
                    help="replica set name", metavar="user")
PARSER.add_argument("-p", "--password", dest="password", required=True,
                    help="password", metavar="passwd")
PARSER.add_argument("-g", "--region", dest="region", required=True,
                    help="region name", metavar="region")
PARSER.add_argument("-i", "--poll-interval", dest="interval", type=int,
                    help="polling interval in seconds", metavar="interval")

PARSER.set_defaults(interval=30)

# ------- globals


def bson_ts_to_long(timestamp):
    """Convert BSON timestamp into integer.

    Conversion rule is based from the specs
    (http://bsonspec.org/#/specification).
    """
    converted_time = (timestamp.time << 32) + timestamp.inc
    return converted_time


class ReplicationLagChecker(object):

    def __init__(self, args):
        """ initialize the args and setup a stats client """
        self._source_host = args.source_host
        self._target_host = args.target_host
        self._replica_set = args.replica_set
        self._user = args.user
        self._password = args.password
        self._poll_interval = args.interval
        self._lag_key = args.region + '_' + args.replica_set + '_lag'
        # We assume a local collectd installation
        self._stat_client = StatsClient()

    def setup_source_db(self):
        """ setup the source mongo connection which is a replica set """
        conn = MongoReplicaSetClient(host=self._source_host,
                                     replicaSet=self._replica_set,
                                     read_preference=ReadPreference.PRIMARY)
        conn['admin'].authenticate(self._user, self._password)
        return conn

    def setup_target_db(self):
        """ setup the target mongo connection which is a standalone client """
        conn = MongoClient(host=self._target_host)
        conn['admin'].authenticate(self._user, self._password)
        return conn

    def run(self):

        """ Check the latest oplog from source oplog collection
            and the latest oplog from target mongo connector collection
            and compute the lag """
        try:
            source_conn = self.setup_source_db()
            target_conn = self.setup_target_db()
            target_collection = 'oplog' + self._replica_set

            while True:
                try:
                    # Induce an operation on the replication test database
                    db_name = 'ReplTest_' + self._replica_set.upper()
                    source_conn[db_name]['operation'].replace_one({'replica': self._replica_set}, {
                                                                  'replica': self._replica_set, 'ts': int(time.time())}, upsert=True)

                    # Wait a bit for it to replicate
                    time.sleep(10)

                    # check latest oplog of source
                    entry = source_conn['local'][
                        'oplog.rs'].find().sort('$natural', -1).limit(1)
                    source_oplog = entry[0]['ts'].time

                    # get latest oplog from connector target oplog collection
                    entry = target_conn['__mongo_connector'][
                        target_collection].find().sort('_ts', -1).limit(1)
                    target_oplog = entry[0]['_ts'] >> 32

                    lag = source_oplog - target_oplog
                    self._stat_client.gauge(self._lag_key, lag)

                    time.sleep(self._poll_interval)
                except Exception as ex:
                    logger.exception('Connection Failed, retrying..')
                    time.sleep(5)

        except Exception as ex:
            logger.exception('Critical Error, bailing out..')

if __name__ == '__main__':
    PARSER_ARGS = PARSER.parse_args()
    FORMAT = '%(asctime)-15s %(message)s'
    SUFFIX = PARSER_ARGS.replica_set+'_'+PARSER_ARGS.region
    LOG_FILENAME = '/var/log/mongo-connector/check_replication_lag_'+SUFFIX
    logging.basicConfig(format=FORMAT,filename=LOG_FILENAME)

    logger = logging.getLogger('check_replication_lag')
    LAG_CHECKER = ReplicationLagChecker(PARSER_ARGS)
    LAG_CHECKER.run()
