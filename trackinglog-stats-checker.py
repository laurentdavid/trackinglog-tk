# A quick checker to see if a given course has got the right amount of events
# TODO: generalise the tool


import argparse
import sys
import os
import re
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import csv

class MongoDBTrackingLogTester:

    def __init__(self, mongodb,course_enrolmment_fname,forumactivity_fname):
        self.mongodb = mongodb
        self.enrolmentsevt = {}
        self.forumevt = {}
        with open(course_enrolmment_fname, 'rb') as csvfile:
            en_reader = csv.reader(csvfile, delimiter=',')
            for row in en_reader:
                self.enrolmentsevt[row[0].replace('/','')]=row[1]

        with open(forumactivity_fname, 'rb') as csvfile:
            en_forum = csv.reader(csvfile, delimiter=',')
            for row in en_forum:
                self.forumevt[row[0].replace('/','')] = row[1]


    def __listCollections(self):
        # Basically we regroup event by date (day) so to make sure
        # that collections keep small. So one day = one collection
        # When we output the data, we will output one file per collection sorted
        col_names = self.mongodb.collection_names(include_system_collections=False)
        return col_names

    def testDay(self, collection, day):
        # Test the number of enrollment
        enr_ct = self.mongodb[collection].find(
            {"event_type":"edx.course.enrollment.activated", "context.course_id":"ENSCachan/20012/session01"}).count()

        enr_st = 0
        if self.enrolmentsevt.has_key(day):
            enr_st = self.enrolmentsevt[day]
        print "'enr',{0},{1},{2}".format(day,enr_st,enr_ct)
        # Test the number of forum thread creation
        forum_ct = self.mongodb[collection].find(
            {"event_type": "edx.forum.thread.created", "context.course_id": "ENSCachan/20012/session01"}).count()
        forum_st = 0
        if self.forumevt.has_key(day):
            forum_st = self.forumevt[day]
        print "'forum',{0},{1},{2}".format(day, forum_st, forum_ct)
    def testEventsFromMongo (self):
        collectionlist = self.__listCollections()
        print "type,day,stats,in_db"
        for col in collectionlist:
            colwithoutc=re.sub(r'c([0-9]+)',r'\1',col)
            if colwithoutc != "other": # we have a collection for a given day then test this day
                self.testDay(col,colwithoutc)



def getCmdArgs():
    parser = argparse.ArgumentParser(description='Export tracking logs to a gzip file, one per collection (day by day) and reordering output.')
    parser.add_argument('--courseid', help='Course ID to check',
                        default='ENSCachan/20012/session01')
    parser.add_argument('--statsbasename', help='Basename for stats',
                        default='ENSCachan_20012_session01')
    parser.add_argument('--mongodserveruri', help='the mongodb server uri to log into', default='mongodb://localhost:27017')
    parser.add_argument('--dbname', help='the mongodb database name to store logs into', default='tldb')

    args = parser.parse_args()
    return args

def checkArgs(args):
    # check if there are course stats for this course

    course_enrolmment_fname = os.path.join("stats",args.statsbasename+"_enrollments.csv")
    forumactivity_fname = os.path.join("stats",args.statsbasename+"_forum-activity.csv")
    if not ( os.path.isfile(course_enrolmment_fname) and os.path.isfile(forumactivity_fname) ) :
        sys.exit("The stats file must exist")
    # check connections with mongodb
    try :
        mclient = MongoClient(args.mongodserveruri)
    except ConnectionFailure as e:
        sys.exit("Mongodb: {0}".format(e.message))
    return course_enrolmment_fname,forumactivity_fname,mclient[args.dbname]

#################### check arguments #########################"""

args = getCmdArgs()

course_enrolmment_fname,forumactivity_fname, mongodb= checkArgs(args)

testerclass = MongoDBTrackingLogTester(mongodb, course_enrolmment_fname,forumactivity_fname)
testerclass.testEventsFromMongo()
