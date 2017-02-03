# Import tracking logs in gz format onto mongo db by walking recursively onto
# all subdirectories of a root path given as an argument of the commandline
# It checks if the given record exist (identical record) before inserting it in the database using
# mongo Hashed index functionality (mongo 3.4 required with pymongo == 3.4

import argparse
import sys
import os
import mimetypes
import gzip
import json
import re
from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure, DuplicateKeyError
import pprint
from datetime import datetime
import hashlib

class ImportStatistics:
    importedFiles = 0
    importedLines = 0
    importErrors = dict()

    def addImportError(self, error, lineNum, filename):
        if (not self.importErrors.has_key(error)):
            self.importErrors[error] = list()
        self.importErrors[error].append({'line': lineNum, 'file': filename })

    def __str__(self):
        return "General Statistics \n " \
               "Imported lines: {0}, Imported Files {1}, Imported Errors {2} \n" \
               "{3}".format(self.importedLines,self.importedFiles,len(self.importErrors),
                      pprint.pformat(self.importErrors))

class MongoDBTrackingLogImporter:

    def __init__(self, mongodb):
        self.mongodb = mongodb
        self.importstats = ImportStatistics()

    def __guessCollectionFromFilename(self, filename):
        # Basically we regroup event by date (day) so to make sure
        # that collections keep small
        datematch = re.compile(r'.*log\-([0-9]+)').match(filename)
        if datematch is None:
            return 'other'
        else:
            return 'c'+datematch.group(1)

    def importSingleTrackingLogEvent(self, linestr, filename, mongocol, linenumber):
        # Check that the event
        try:
            data = json.loads(linestr, strict=False)
            data['file_orig']=filename # we keep the filename
            # Here we insert an event ONLY if it does not already exist
            # We assume that an even exist already in the database if it has the same
            # common fields (http://edx.readthedocs.io/projects/devdata/en/latest/internal_data_formats/tracking_logs.html#common-fields)
            #
            #myfilter = {}
            #for commonfield in ['accept_language','agent','context','event','event_source','event_type','host','ip','referer','time','username']:
            #   myfilter[commonfield] =  data[commonfield]

            # Compute a hash so to check unicity of the entry and prevent duplication of the same line
            # of log
            data['hash']= hashlib.sha1(linestr.encode()).hexdigest()
            self.mongodb[mongocol].insert(data)
            self.importstats.importedLines += 1
        except ValueError as e:
            print ("Not able to parse the json ({0}) at line {1}:  {2} ".format(e.message, linenumber, linestr.encode('utf-8')))
            # Try to recover error before "giving up"
            self.importstats.addImportError(e.message,linenumber,filename)
        except DuplicateKeyError as e:
            print ("Data already inserted in the database ({0}) at line {1}:  {2} ".format(e.message, linenumber, linestr.encode('utf-8')))

    def importAFileInMongo (self,filepath,filename):
        # We consider that logs files are of the type gzip
        self.importstats.importedFiles += 1

        linenumber = 0
        fullpath = os.path.join(filepath,filename)
        mongocol = self.__guessCollectionFromFilename(filename)
        self.mongodb[mongocol].create_index("hash",unique=True) # Make sure that entries are unique
        self.mongodb[mongocol].create_index([("time", ASCENDING)], unique=False)
        if ('gzip' in mimetypes.guess_type(fullpath)):
            # check that the file is not already processed in previous runs (sincedb)
            # Process the file
            with gzip.open(fullpath, 'rb') as trackingfile:
                for line in trackingfile:
                    self.importSingleTrackingLogEvent(line.decode('utf-8'),filename, mongocol,linenumber)
                    linenumber = linenumber + 1
        else:
            print ("Skip file {0}".format(filepath))

    def getImportStatistics(self):
        return self.importstats

def getCmdArgs():
    parser = argparse.ArgumentParser(description='Import tracking logs from a given root path by looking recursively at each .gz files and importing them into the given database.')
    parser.add_argument('logsdir', help='the path to the directory containing logs')
    parser.add_argument('--mongodserveruri', help='the mongodb server uri to log into', default='mongodb://localhost:27017')
    parser.add_argument('--dbname', help='the mongodb database name to store logs into', default='tldb')

    args = parser.parse_args()
    return args

def checkArgs(args):
    # check logpath
    logsdir = os.path.normpath(args.logsdir)
    if logsdir is '' or not os.path.exists(logsdir):
        sys.exit("The logsdir dir must be a valid path")
    # check connections with mongodb
    try :
        mclient = MongoClient(args.mongodserveruri)
    except ConnectionFailure as e:
        sys.exit("Mongodb: {0}".format(e.message))
    return logsdir,mclient[args.dbname]

#################### check arguments #########################"""

args = getCmdArgs()

logsdir, mongodb= checkArgs(args)

startTime = datetime.now()

mongoimporter =  MongoDBTrackingLogImporter(mongodb= mongodb)
# Walk and parse json tracking logs
for dirName, subdirList, fileList in os.walk(args.logsdir):
    print('Found directory: %s' % dirName)
    # exclude non gz files
    fileList = [f for f in fileList if re.match(r'.*\.gz',f)]
    for fname in fileList:
        print('\tImporting %s' % fname)
        mongoimporter.importAFileInMongo(os.path.join(logsdir,dirName), fname)

print mongoimporter.getImportStatistics()

print "Time taken"
print datetime.now() - startTime
