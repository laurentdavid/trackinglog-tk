# Import tracking logs in gz format onto mongo db by walking recursively onto
# all subdirectories of a root path given as an argument of the commandline
# It checks if the given record exist (identical record) before inserting it in the database

import argparse
import sys
import os
import gzip
import json
import re
from pymongo import MongoClient
from pymongo import ASCENDING
from pymongo.errors import ConnectionFailure

class ExportStatistics:
    exportedFiles = 0
    exportedLines = 0

    def __str__(self):
        return "General Statistics \n " \
               "Exported files: {0}, Exported events {1} \n".format(self.exportedFiles,self.exportedLines)


class MongoDBTrackingLogExporter:

    def __init__(self, mongodb):
        self.mongodb = mongodb
        self.exportstats =  ExportStatistics()

    def __listCollections(self):
        # Basically we regroup event by date (day) so to make sure
        # that collections keep small. So one day = one collection
        # When we output the data, we will output one file per collection sorted
        col_names = self.mongodb.collection_names(include_system_collections=False)

        return col_names

    def exportCollection(self, collectionname, filenameout):
        with  gzip.open(filenameout, 'w') as f:
            for doc in self.mongodb[collectionname].find(projection={"hash":0,"_id":0}).sort('time',ASCENDING):
                f.write(json.dumps(doc)+"\n")
                self.exportstats.exportedLines += 1

    def exportFilesFromMongo (self,filepath):
        collectionlist = self.__listCollections()
        for col in collectionlist:
            colwithoutc=re.sub(r'c([0-9]+)',r'\1',col)
            filefullpath = os.path.join(filepath,
                         'tracking.log-' + colwithoutc + '.gz')
            print ("Indexing collection with the right sort field".format(col))
            self.mongodb[col].create_index([("time",ASCENDING)], unique=False,background=False)
            print ("Exporting to file {0}".format(filefullpath))
            self.exportCollection(col,filefullpath)
            self.exportstats.exportedFiles += 1

    def getExportStatistics(self):
        return self.exportstats

def getCmdArgs():
    parser = argparse.ArgumentParser(description='Export tracking logs to a gzip file, one per collection (day by day) and reordering output.')
    parser.add_argument('outputdir', help='the path to the directory containing gzipped logs')
    parser.add_argument('--mongodserveruri', help='the mongodb server uri to log into', default='mongodb://localhost:27017')
    parser.add_argument('--dbname', help='the mongodb database name to store logs into', default='tldb')

    args = parser.parse_args()
    return args

def checkArgs(args):
    # check logpath
    outputdir = os.path.normpath(args.outputdir)
    if outputdir is '' or not os.path.exists(outputdir):
        sys.exit("The outputdir dir must be a valid path")
    # check connections with mongodb
    try :
        mclient = MongoClient(args.mongodserveruri)
    except ConnectionFailure as e:
        sys.exit("Mongodb: {0}".format(e.message))
    return outputdir,mclient[args.dbname]

#################### check arguments #########################"""

args = getCmdArgs()

outputdir, mongodb= checkArgs(args)

mongoexporter =  MongoDBTrackingLogExporter(mongodb= mongodb)
# Walk and parse json tracking logs
mongoexporter.exportFilesFromMongo(outputdir)


print mongoexporter.getExportStatistics()