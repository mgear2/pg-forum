# pylint: disable=import-error
import xml.etree.ElementTree as ET
from connector import Connector
import psycopg2
import datetime
import re

class DBbuilder():
    def __init__(self, connector, lims):
        # reference for connector object
        self.connector = connector
        # define roots for each xml file
        self.tagsroot = ET.parse('app/data/woodworking.stackexchange.com/Tags.xml').getroot()
        self.usersroot = ET.parse('app/data/woodworking.stackexchange.com/Users.xml').getroot()
        self.postsroot = ET.parse('app/data/woodworking.stackexchange.com/Posts.xml').getroot()
        self.commentsroot = ET.parse('app/data/woodworking.stackexchange.com/Comments.xml').getroot()
        self.badgesroot = ET.parse('app/data/woodworking.stackexchange.com/Badges.xml').getroot()
         # entity table creation statements
        self.createtags = 'CREATE TABLE Tags (tag_id int PRIMARY KEY, tag_name varchar({0}))'.format(lims[0])
        self.createusers = 'CREATE TABLE Users (user_id int PRIMARY KEY, user_name varchar({0}), location varchar({0}), reputation int, creation_date timestamp, last_active_date timestamp, about varchar({1}))'.format(lims[0], lims[3])
        self.createposts = 'CREATE TABLE Posts (post_id int PRIMARY KEY, creation_date timestamp, last_edit_date timestamp, favorite_count int, view_count int, score int, title varchar({0}), body varchar({1}))'.format(lims[0], lims[3])
        self.createcomments = 'CREATE TABLE Comments (comment_id int PRIMARY KEY, score int, creation_date timestamp, text varchar({0}))'.format(lims[3])
        self.createbadges = 'CREATE TABLE Badges (badge_id int PRIMARY KEY, badge_name varchar({0}))'.format(lims[0])
        # relationship table creation statements
        self.createposted = 'CREATE TABLE Posted (user_id int REFERENCES Users(user_id), post_id int REFERENCES Posts(post_id))'
        self.createtagged = 'CREATE TABLE Tagged (tag_id int REFERENCES Tags(tag_id), post_id int REFERENCES Posts(post_id))'
        self.createcommented = 'CREATE TABLE Commented (user_id int REFERENCES Users(user_id), comment_id int REFERENCES Comments(comment_id))'
        self.createthread = 'CREATE TABLE Thread (post_id int REFERENCES Posts(post_id), comment_id int REFERENCES Comments(comment_id))'
        self.createdecorated = 'CREATE TABLE Decorated (user_id int REFERENCES Users(user_id), badge_id int REFERENCES Badges(badge_id), date_awarded timestamp)'
        self.createsubposts = 'CREATE TABLE Subposts (parent_id int REFERENCES Posts(post_id), child_id int REFERENCES Posts(post_id))'
        self.createsubcomments = 'CREATE TABLE Subcomments (parent_id int REFERENCES Comments(comment_id), child_id int REFERENCES Comments(comment_id))'

        self.createstoredproc = "CREATE OR REPLACE PROCEDURE newpost(post_id int, creation_date timestamp, last_edit_date timestamp, \
                                favorite_count int, view_count int, score int, title varchar(100), body varchar(5000)) AS $$\
                                BEGIN\
                                INSERT INTO Posts (post_id, creation_date, last_edit_date, favorite_count, view_count, score, title, body)\
                                     VALUES (post_id, creation_date, last_edit_date, favorite_count, view_count, score, title, body);\
                                END;\
                                $$ LANGUAGE plpgsql;"
        # define limit values
        self.lims = lims
        # keys referenced by foreign keys need to be checked to ensure that insert queries will not fail because on a missing pkey. Storing these values client side increases build speed. 
        self.post_ids = []
        self.user_ids = []
        self.user_names = []
        self.comment_ids = []
        self.badge_ids = []

    def createadmin(self):
        adminstring = "INSERT INTO Users (user_id, user_name, location, reputation, creation_date, last_active_date, about) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        tuples = -999, "Admin", "Wherever", 999999, datetime.datetime.now(), datetime.datetime.now(), "This is too much responsibility"
        self.connector.operate(adminstring, tuples)

    def count(self, table):
        count = self.connector.operate("SELECT COUNT(*) FROM {0}".format(table), None)
        count = (count[0])[0]
        print ("Initialization for table {0} complete with {1} rows.".format(table, count))

    def buildtags(self):
        print ("Initializing table Tags...this may take a few minutes")
        self.connector.operate(self.createtags, None)

        string = "INSERT INTO Tags (tag_id, tag_name) VALUES "
        tuples = ()
        i = 0

        for type_tag in self.tagsroot.findall('row'):
            tag_id = type_tag.get('Id')
            tag_name = type_tag.get('TagName')

            if int(tag_id) > self.lims[1]:
                break
            if len(str(tag_name)) > self.lims[0]:
                continue
            if i > 0:
                string += ","
            string += "(%s, %s)"
            tuples += tag_id, tag_name
            i += 1

        self.connector.operate(string, tuples)
        self.count("Tags")

    def buildusers(self):
        print ("Initializing table Users...this may take a few minutes")
        self.connector.operate(self.createusers, None)

        string = "INSERT INTO Users (user_id, user_name, location, reputation, creation_date, last_active_date, about) VALUES "
        tuples = ()
        i = 0

        for type_tag in self.usersroot.findall('row'):
            user_id = type_tag.get('Id')
            user_name = type_tag.get('DisplayName')
            reputation = type_tag.get('Reputation')
            creation_date = type_tag.get('CreationDate')
            last_active_date = type_tag.get('LastAccessDate')
            location = type_tag.get('Location')
            about = type_tag.get('AboutMe')

            if int(user_id) > self.lims[2]:
                break
            if len(str(about)) > lims[3] or len(str(user_name)) > lims[0]:
                continue
            if user_name in self.user_names:
                break
            if i > 0:
                string += ","
            string += "(%s, %s, %s, %s, %s, %s, %s)"
            tuples += user_id, user_name, location, reputation, datetime.datetime.strptime(creation_date, '%Y-%m-%dT%H:%M:%S.%f'), datetime.datetime.strptime(last_active_date, '%Y-%m-%dT%H:%M:%S.%f'), about
            self.user_ids.append(int(user_id))
            self.user_names.append(user_name)
            i += 1

        self.connector.operate(string, tuples)
        self.count("Users")

    # cool little function from https://stackoverflow.com/questions/9662346/python-code-to-remove-html-tags-from-a-string using regex
    def cleanhtml(self, raw_html):
        cleanr = re.compile('<.*?>')
        cleantext = re.sub(cleanr, '', raw_html)
        return cleantext

    def buildposts(self):
        print ("Initializing table Posts...this may take a few minutes")
        self.connector.operate(self.createposts, None)

        string = "INSERT INTO Posts (post_id, creation_date, last_edit_date, favorite_count, view_count, score, title, body) VALUES "
        tuples = ()
        i = 0

        for type_tag in self.postsroot.findall('row'):
            post_id = type_tag.get('Id')
            creation_date = type_tag.get('CreationDate')
            last_edit_date = type_tag.get('LastEditDate')
            favorite_count = type_tag.get('FavoriteCount')
            view_count = type_tag.get('ViewCount')
            title = type_tag.get('Title')
            raw_html = type_tag.get('Body')
            score = type_tag.get('Score')
            # remove html tags and newlines/carriage returns from post body
            clean = self.cleanhtml(raw_html)
            body = clean.replace('\n', ' ').replace('\r', ' ')

            if int(post_id) > self.lims[2]:
                break
            if len(str(title)) > lims[0] or len(str(body)) > lims[3]:
                continue
            if i > 0:
                string += ","
            if last_edit_date == None:
                last_edit_date = creation_date    
            string += "(%s, %s, %s, %s, %s, %s, %s, %s)"
            tuples += post_id, datetime.datetime.strptime(creation_date, '%Y-%m-%dT%H:%M:%S.%f'), datetime.datetime.strptime(last_edit_date, '%Y-%m-%dT%H:%M:%S.%f'), favorite_count, view_count, score, title, body
            self.post_ids.append(int(post_id))
            i += 1

        self.connector.operate(string, tuples)
        self.count("Posts")

    # due to the the design of stackexchange sites and resulting data, subposts must be handled separately from comments and subcomments
    def buildsubposts(self):
        print ("Initializing table Subposts...this may take a few minutes")
        self.connector.operate(self.createsubposts, None)

        string = "INSERT INTO Subposts (parent_id, child_id) VALUES "
        tuples = ()
        i = 0

        for type_tag in self.postsroot.findall('row'):
            parent_id = type_tag.get('ParentId')
            child_id = type_tag.get('Id')
            if int(child_id) > self.lims[2]:
                break
            if parent_id == None or int(parent_id) not in self.post_ids or int(child_id) not in self.post_ids:
                continue
            if i > 0:
                string += ","
            string += "(%s, %s)"
            tuples += parent_id, child_id
            i += 1
        
        self.connector.operate(string, tuples)
        self.count("Subposts")

    def buildposted(self):
        print ("Initializing table Posted...this may take a few minutes")
        self.connector.operate(self.createposted, None)

        string = "INSERT INTO Posted (user_id, post_id) VALUES "
        tuples = ()
        i = 0

        for type_tag in self.postsroot.findall('row'):
            post_id = type_tag.get('Id')
            owner_id = type_tag.get('OwnerUserId')

            if(owner_id == None):
                continue
            if int(post_id) > self.lims[2]:
                break
            if int(post_id) not in self.post_ids or int(owner_id) not in self.user_ids:
                continue
            if i > 0:
                string += ","
            string += "(%s, %s)"
            tuples += owner_id, post_id
            i += 1

        self.connector.operate(string, tuples)
        self.count("Posted")

    def buildtagged(self):
        print ("Initializing table Tagged...this may take a few minutes")
        self.connector.operate(self.createtagged, None)

        string = "INSERT INTO Tagged (tag_id, post_id) VALUES "
        tuples = ()
        i = 0

        for type_tag in self.postsroot.findall('row'):
            post_id = type_tag.get('Id')
            tags = type_tag.get('Tags')
            if int(post_id) > self.lims[2]:
                break
            if int(post_id) not in self.post_ids:
                continue

            if tags != None:
                tags = tags.split("><")
                for tag in tags:
                    if "<" in tag:
                        list(tag)
                        tag = tag[1:]
                        tag = ''.join(tag)
                    if ">" in tag:
                        list(tag)
                        tag = tag[:(len(tag)-1)]
                        tag = ''.join(tag)

                    tag_id = self.connector.operate('SELECT tag_id FROM Tags WHERE tag_name = \'{0}\''.format(tag), None)
                    if tag_id == []:
                        continue
                    tag_id = ((tag_id[0])[0])
                    if i > 0:
                        string += ","
                    string += "(%s, %s)"
                    tuples += tag_id, post_id
                    i += 1

        self.connector.operate(string, tuples)
        self.count( "Tagged")

    def buildcomments(self):
        print ("Initializing table Comments...this may take a few minutes")
        self.connector.operate(self.createcomments, None)

        string = "INSERT INTO Comments (comment_id, score, creation_date, text) VALUES "
        tuples = ()
        i = 0

        for type_tag in self.commentsroot.findall('row'):
            comment_id = type_tag.get('Id')
            creation_date = type_tag.get('CreationDate')
            score = type_tag.get('Score')
            text = type_tag.get('Text')
            if int(comment_id) > self.lims[2]:
                break
            if i > 0:
                string += ","
            string += "(%s, %s, %s, %s)"
            tuples += comment_id, score, datetime.datetime.strptime(creation_date, '%Y-%m-%dT%H:%M:%S.%f'), text
            self.comment_ids.append(int(comment_id))
            i += 1

        self.connector.operate(string, tuples)
        self.count("Comments")

    # stackexhange sites do not account/allow for nested comments
    # this app implements a table to allow for infinitely nested subcomments, but these will not be derived from stackexchange data
    def buildsubcomments(self):
        print ("Initializing table Subcomments...this may take a few minutes")
        self.connector.operate(self.createsubcomments, None)
        self.count("Subcomments")

    def buildcommented(self):
        print ("Initializing table Commented...this may take a few minutes")
        self.connector.operate(self.createcommented, None)

        string = "INSERT INTO Commented (user_id, comment_id) VALUES  "
        tuples = ()
        i = 0

        for type_tag in self.commentsroot.findall('row'):
            comment_id = type_tag.get('Id')
            user_id = type_tag.get('UserId')
            if int(comment_id) > self.lims[2]:
                break
            if int(user_id) not in self.user_ids or int(comment_id) not in self.comment_ids:
                continue
            if i > 0:
                string += ","
            string += "(%s, %s)"
            tuples += user_id, comment_id
            i += 1

        self.connector.operate(string, tuples)
        self.count("Commented")

    def buildthread(self):
        print ("Initializing table Thread...this may take a few minutes")
        self.connector.operate(self.createthread, None)

        string = "INSERT INTO Thread (post_id, comment_id) VALUES "
        tuples = ()
        i = 0

        for type_tag in self.commentsroot.findall('row'):
            comment_id = type_tag.get('Id')
            post_id = type_tag.get('PostId')
            if int(comment_id) > self.lims[2]:
                break
            if int(comment_id) not in self.comment_ids or int(post_id) not in self.post_ids:
                continue
            if i > 0:
                string += ","
            string += "(%s, %s)"
            tuples += post_id, comment_id
            i += 1

        self.connector.operate(string, tuples)
        self.count("Thread")

    def buildbadges(self):
        print ("Initializing table Badges...this may take a few minutes")
        self.connector.operate(self.createbadges, None)

        string = "INSERT INTO Badges (badge_id, badge_name) VALUES "
        tuples = ()
        i = 0

        for type_tag in self.badgesroot.findall('row'):
            badge_id = type_tag.get('Id')
            badge_name = type_tag.get('Name')
            if int(badge_id) > self.lims[2]:
                break
            if i > 0:
                string += ","
            string += "(%s, %s)"
            tuples += badge_id, badge_name
            self.badge_ids.append(int(badge_id))
            i += 1

        self.connector.operate(string, tuples)
        self.count("Badges")

    def builddecorated(self):
        print ("Initializing table Decorated...this may take a few minutes")
        self.connector.operate(self.createdecorated, None)

        string = "INSERT INTO Decorated (user_id, badge_id, date_awarded) VALUES "
        tuples = ()
        i = 0

        for type_tag in self.badgesroot.findall('row'):
            user_id = type_tag.get('UserId')
            badge_id = type_tag.get('Id')
            date_awarded = type_tag.get('Date')
            if int(badge_id) > self.lims[2]:
                break
            if int(user_id) not in self.user_ids or int(badge_id) not in self.badge_ids:
                continue
            if i > 0:
                string += ","
            string += "(%s, %s, %s)"
            tuples += user_id, badge_id, datetime.datetime.strptime(date_awarded, '%Y-%m-%dT%H:%M:%S.%f')
            i += 1

        self.connector.operate(string, tuples)
        self.count("Decorated")

if __name__ == '__main__':
    # create a Connector instance and connect to the database
    connector = Connector()
    connector.connect()

    # initialize dbbuilder
    lims = [100, 200, 400, 5000]
    dbbuilder = DBbuilder(connector, lims)
    
    # create entity tables
    dbbuilder.buildtags()
    dbbuilder.buildusers()
    dbbuilder.buildposts()
    dbbuilder.buildcomments()
    dbbuilder.buildbadges()

    # create relationship tables
    dbbuilder.buildposted()
    dbbuilder.buildtagged()
    dbbuilder.buildcommented()
    dbbuilder.buildthread()
    dbbuilder.builddecorated()
    dbbuilder.buildsubposts()
    dbbuilder.buildsubcomments()

    dbbuilder.createadmin()
    connector.operate(dbbuilder.createstoredproc, None)

    # disconnect from the database
    connector.disconnect()