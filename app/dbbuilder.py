# pylint: disable=import-error
import xml.etree.ElementTree as ET
from connector import Connector
import psycopg2
from datetime import datetime
import re

class DBbuilder():
    def __init__(self, connector, lims):
        # reference for connector object
        self.connector = connector
        # define roots for each xml file
        self.tagsroot = ET.parse(
            'app/data/woodworking.stackexchange.com/Tags.xml').getroot()
        self.usersroot = ET.parse(
            'app/data/woodworking.stackexchange.com/Users.xml').getroot()
        self.postsroot = ET.parse(
            'app/data/woodworking.stackexchange.com/Posts.xml').getroot()
        self.commentsroot = ET.parse(
            'app/data/woodworking.stackexchange.com/Comments.xml').getroot()
        self.badgesroot = ET.parse(
            'app/data/woodworking.stackexchange.com/Badges.xml').getroot()
         # entity table creation statements
        self.createtags = (
            "CREATE TABLE Tags ("
            "tag_id int PRIMARY KEY,"
            "tag_name varchar({0}))".format(lims[0])
            )
        self.createusers = (
            "CREATE TABLE Users ("
            "user_id int PRIMARY KEY,"
            "user_name varchar({0}),"
            "location varchar({0}),"
            "reputation int,"
            "creation_date timestamp,"
            "last_active_date timestamp,"
            "about varchar({1}))".format(lims[0], lims[3])
            )
        self.createposts = (
            "CREATE TABLE Posts ("
            "post_id int PRIMARY KEY,"
            "creation_date timestamp,"
            "last_edit_date timestamp,"
            "favorite_count int,"
            "view_count int,"
            "score int,"
            "title varchar({0})," 
            "body varchar({1}))".format(lims[0], lims[3])
            )
        self.createcomments = (
            "CREATE TABLE Comments ("
            "comment_id int PRIMARY KEY,"
            "score int,"
            "creation_date timestamp,"
            "text varchar({0}))".format(lims[3])
            )
        self.createbadges = (
            "CREATE TABLE Badges ("
            "badge_id int PRIMARY KEY,"
            "badge_name varchar({0}))".format(lims[0])
            )
        # relationship table creation statements
        self.createposted = (
            "CREATE TABLE Posted ("
            "user_id int REFERENCES Users(user_id),"
            "post_id int REFERENCES Posts(post_id))"
            )
        self.createtagged = (
            "CREATE TABLE Tagged ("
            "tag_id int REFERENCES Tags(tag_id),"
            "post_id int REFERENCES Posts(post_id))"
            )
        self.createcommented = (
            "CREATE TABLE Commented ("
            "user_id int REFERENCES Users(user_id),"
            "comment_id int REFERENCES Comments(comment_id))"
            )
        self.createthread = (
            "CREATE TABLE Thread ("
            "post_id int REFERENCES Posts(post_id),"
            "comment_id int REFERENCES Comments(comment_id))"
            )
        self.createdecorated = (
            "CREATE TABLE Decorated ("
            "user_id int REFERENCES Users(user_id),"
            "badge_id int REFERENCES Badges(badge_id),"
            "date_awarded timestamp)"
            )
        self.createsubposts = (
            "CREATE TABLE Subposts ("
            "parent_id int REFERENCES Posts(post_id),"
            "child_id int REFERENCES Posts(post_id))"
            )
        self.createsubcomments = (
            "CREATE TABLE Subcomments ("
            "parent_id int REFERENCES Comments(comment_id),"
            "child_id int REFERENCES Comments(comment_id))"
            )
        # stored procedure creation statements
        self.newpostproc = (
            "CREATE OR REPLACE PROCEDURE newpost("
                "post_id int, creation_date timestamp, last_edit_date timestamp,"
                "favorite_count int, view_count int, score int,"
                "title varchar(100), body varchar(5000))"
            "AS $$"
            "BEGIN "
            "INSERT INTO Posts ("
                "post_id, creation_date, last_edit_date,"
                "favorite_count, view_count, score,"
                "title, body)"
            "VALUES ("
                "post_id, creation_date, last_edit_date,"
                "favorite_count, view_count, score,"
                "title, body);"
            "END;"
            "$$ LANGUAGE plpgsql;"
            )
        # Limits are used to prevent invalid insertions and/or limit DB size. 
        self.lims = lims
        self.datefmt = '%Y-%m-%dT%H:%M:%S.%f'
        # Insert queries can fail en masse because of a missing pkey. 
        # Storing and checking these values prevents this.
        # Doing this client side increases build speed. 
        self.post_ids = []
        self.user_ids = []
        self.user_names = []
        self.comment_ids = []
        self.badge_ids = []

    def build(self):
         # Entity tables
        self.buildtags()
        self.buildusers()
        self.buildposts()
        self.buildcomments()
        self.buildbadges()

        # Relationship tables
        self.buildposted()
        self.buildtagged()
        self.buildcommented()
        self.buildthread()
        self.builddecorated()
        self.buildsubposts()
        self.buildsubcomments()
        
        self.createadmin()
        self.connector.operate(dbbuilder.newpostproc, None)

    def createadmin(self):
        adminstring = (
            "INSERT INTO Users ("
            "user_id, user_name, location, reputation,"
            "creation_date, last_active_date,"
            "about)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s)"
            )
        admintuples = (
            -999, "Admin", "Wherever", 999999, 
            datetime.now(), datetime.now(), 
            "This is too much responsibility"
            )
        self.connector.operate(adminstring, admintuples)

    def count(self, table):
        countstring = "SELECT COUNT(*) FROM {0}".format(table)
        count = self.connector.operate(countstring, None)
        count = (count[0])[0]
        print ("Initialization for table {0} complete with {1} rows.".format(table, count))

    def buildtags(self):
        print ("Initializing table Tags...this may take a few minutes")
        self.connector.operate(self.createtags, None)

        tagstring = "INSERT INTO Tags (tag_id, tag_name) VALUES "
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
                tagstring += ","
            tagstring += "(%s, %s)"
            tuples += tag_id, tag_name
            i += 1

        self.connector.operate(tagstring, tuples)
        self.count("Tags")

    def buildusers(self):
        print ("Initializing table Users...this may take a few minutes")
        self.connector.operate(self.createusers, None)

        userstring = (
            "INSERT INTO Users ("
            "user_id, user_name, location, reputation,"
            "creation_date, last_active_date, about) VALUES "
            )
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
                userstring += ","
            userstring += "(%s, %s, %s, %s, %s, %s, %s)"
            tuples += (
                user_id, user_name, location, reputation,
                datetime.strptime(creation_date, self.datefmt),
                datetime.strptime(last_active_date, self.datefmt),
                about
                )
            self.user_ids.append(int(user_id))
            self.user_names.append(user_name)
            i += 1

        self.connector.operate(userstring, tuples)
        self.count("Users")

    # Regex function from stackoverflow
    # https://stackoverflow.com/questions/9662346/python-code-to-remove-html-tags-from-a-string
    def cleanhtml(self, raw_html):
        cleanr = re.compile('<.*?>')
        cleantext = re.sub(cleanr, '', raw_html)
        return cleantext

    def buildposts(self):
        print ("Initializing table Posts...this may take a few minutes")
        self.connector.operate(self.createposts, None)

        poststring = (
            "INSERT INTO Posts ("
            "post_id,"
            "creation_date, last_edit_date,"
            "favorite_count, view_count, score, title, body) VALUES "
            )
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
            if (len(str(title)) > lims[0] 
                    or len(str(body)) > lims[3]):
                continue
            if i > 0:
                poststring += ","
            if last_edit_date == None:
                last_edit_date = creation_date    
            poststring += "(%s, %s, %s, %s, %s, %s, %s, %s)"
            tuples += (
                post_id, 
                datetime.strptime(creation_date, self.datefmt), 
                datetime.strptime(last_edit_date, self.datefmt), 
                favorite_count, view_count, score, title, body
                )
            self.post_ids.append(int(post_id))
            i += 1

        self.connector.operate(poststring, tuples)
        self.count("Posts")

    # Subposts must be handled separately from comments and subcomments.
    # This is an artifact of stackexchange site design. 
    def buildsubposts(self):
        print ("Initializing table Subposts...this may take a few minutes")
        self.connector.operate(self.createsubposts, None)

        subpoststring = "INSERT INTO Subposts (parent_id, child_id) VALUES "
        tuples = ()
        i = 0

        for type_tag in self.postsroot.findall('row'):
            parent_id = type_tag.get('ParentId')
            child_id = type_tag.get('Id')
            if int(child_id) > self.lims[2]:
                break
            if (parent_id == None 
                    or int(parent_id) not in self.post_ids 
                    or int(child_id) not in self.post_ids):
                continue
            if i > 0:
                subpoststring += ","
            subpoststring += "(%s, %s)"
            tuples += parent_id, child_id
            i += 1
        
        self.connector.operate(subpoststring, tuples)
        self.count("Subposts")

    def buildposted(self):
        print ("Initializing table Posted...this may take a few minutes")
        self.connector.operate(self.createposted, None)

        postedstring = "INSERT INTO Posted (user_id, post_id) VALUES "
        tuples = ()
        i = 0

        for type_tag in self.postsroot.findall('row'):
            post_id = type_tag.get('Id')
            owner_id = type_tag.get('OwnerUserId')

            if owner_id == None:
                continue
            if int(post_id) > self.lims[2]:
                break
            if (int(post_id) not in self.post_ids 
                    or int(owner_id) not in self.user_ids):
                continue
            if i > 0:
                postedstring += ","
            postedstring += "(%s, %s)"
            tuples += owner_id, post_id
            i += 1

        self.connector.operate(postedstring, tuples)
        self.count("Posted")

    def buildtagged(self):
        print ("Initializing table Tagged...this may take a few minutes")
        self.connector.operate(self.createtagged, None)

        taggedstring = "INSERT INTO Tagged (tag_id, post_id) VALUES "
        gettag = "SELECT tag_id FROM Tags WHERE tag_name = \'{0}\'"
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
                    tag_id = self.connector.operate(gettag.format(tag), None)
                    if tag_id == []:
                        continue
                    tag_id = ((tag_id[0])[0])
                    if i > 0:
                        taggedstring += ","
                    taggedstring += "(%s, %s)"
                    tuples += tag_id, post_id
                    i += 1

        self.connector.operate(taggedstring, tuples)
        self.count( "Tagged")

    def buildcomments(self):
        print ("Initializing table Comments...this may take a few minutes")
        self.connector.operate(self.createcomments, None)

        commentstring = (
            "INSERT INTO Comments ("
            "comment_id, score, creation_date, text) VALUES "
            )
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
                commentstring += ","
            commentstring += "(%s, %s, %s, %s)"
            tuples += (
                comment_id, score, 
                datetime.strptime(creation_date, self.datefmt), text
                )
            self.comment_ids.append(int(comment_id))
            i += 1

        self.connector.operate(commentstring, tuples)
        self.count("Comments")

    # Stackexhange sites do not account/allow for nested comments.
    # This app implements a table to allow for infinitely nested subcomments.
    def buildsubcomments(self):
        print ("Initializing table Subcomments...this may take a few minutes")
        self.connector.operate(self.createsubcomments, None)
        self.count("Subcomments")

    def buildcommented(self):
        print ("Initializing table Commented...this may take a few minutes")
        self.connector.operate(self.createcommented, None)

        commentedstring = "INSERT INTO Commented (user_id, comment_id) VALUES  "
        tuples = ()
        i = 0

        for type_tag in self.commentsroot.findall('row'):
            comment_id = type_tag.get('Id')
            user_id = type_tag.get('UserId')
            if int(comment_id) > self.lims[2]:
                break
            if (int(user_id) not in self.user_ids 
                    or int(comment_id) not in self.comment_ids):
                continue
            if i > 0:
                commentedstring += ","
            commentedstring += "(%s, %s)"
            tuples += user_id, comment_id
            i += 1

        self.connector.operate(commentedstring, tuples)
        self.count("Commented")

    def buildthread(self):
        print ("Initializing table Thread...this may take a few minutes")
        self.connector.operate(self.createthread, None)

        threadstring = "INSERT INTO Thread (post_id, comment_id) VALUES "
        tuples = ()
        i = 0

        for type_tag in self.commentsroot.findall('row'):
            comment_id = type_tag.get('Id')
            post_id = type_tag.get('PostId')
            if int(comment_id) > self.lims[2]:
                break
            if (int(comment_id) not in self.comment_ids 
                    or int(post_id) not in self.post_ids):
                continue
            if i > 0:
                threadstring += ","
            threadstring += "(%s, %s)"
            tuples += post_id, comment_id
            i += 1

        self.connector.operate(threadstring, tuples)
        self.count("Thread")

    def buildbadges(self):
        print ("Initializing table Badges...this may take a few minutes")
        self.connector.operate(self.createbadges, None)

        badgestring = "INSERT INTO Badges (badge_id, badge_name) VALUES "
        tuples = ()
        i = 0

        for type_tag in self.badgesroot.findall('row'):
            badge_id = type_tag.get('Id')
            badge_name = type_tag.get('Name')
            if int(badge_id) > self.lims[2]:
                break
            if i > 0:
                badgestring += ","
            badgestring += "(%s, %s)"
            tuples += badge_id, badge_name
            self.badge_ids.append(int(badge_id))
            i += 1

        self.connector.operate(badgestring, tuples)
        self.count("Badges")

    def builddecorated(self):
        print ("Initializing table Decorated...this may take a few minutes")
        self.connector.operate(self.createdecorated, None)

        decoratedstring = (
            "INSERT INTO Decorated ("
            "user_id, badge_id, date_awarded) VALUES "
            )
        tuples = ()
        i = 0

        for type_tag in self.badgesroot.findall('row'):
            user_id = type_tag.get('UserId')
            badge_id = type_tag.get('Id')
            date_awarded = type_tag.get('Date')
            if int(badge_id) > self.lims[2]:
                break
            if (int(user_id) not in self.user_ids 
                    or int(badge_id) not in self.badge_ids):
                continue
            if i > 0:
                decoratedstring += ","
            decoratedstring += "(%s, %s, %s)"
            tuples += (
                user_id, badge_id, 
                datetime.strptime(date_awarded, self.datefmt)
                )
            i += 1

        self.connector.operate(decoratedstring, tuples)
        self.count("Decorated")

if __name__ == '__main__':
    connector = Connector()
    connector.connect()

    lims = [100, 200, 400, 5000]
    dbbuilder = DBbuilder(connector, lims)
    dbbuilder.build()

    connector.disconnect()