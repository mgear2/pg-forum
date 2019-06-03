# pylint: disable=import-error
import xml.etree.ElementTree as ET
from connector import Connector
import psycopg2
import datetime 

class DBbuilder():
    def __init__(self):
        self.tagsroot = ET.parse('app/data/woodworking.stackexchange.com/Tags.xml').getroot()
        self.usersroot = ET.parse('app/data/woodworking.stackexchange.com/Users.xml').getroot()
        self.postsroot = ET.parse('app/data/woodworking.stackexchange.com/Posts.xml').getroot()
        self.commentsroot = ET.parse('app/data/woodworking.stackexchange.com/Comments.xml').getroot()
        self.badgesroot = ET.parse('app/data/woodworking.stackexchange.com/Badges.xml').getroot()

    def buildtags(self, connector, table, create):
        print ("Initializing table Tags...this may take a few minutes")
        print(self.tagsroot.tag)
        connector.operate(create, None)
        for type_tag in self.tagsroot.findall('row'):
            tag_id = type_tag.get('Id')
            tag_name = type_tag.get('TagName')
            string = "INSERT INTO Tags (tag_id, tag_name) VALUES (%s, %s)"
            connector.operate(string, (tag_id, tag_name))

        print ("Initialization for table Tags complete.")

    def buildusers(self, connector, create):
        print ("Initializing table Users...this may take a few minutes")
        print(self.usersroot.tag)
        connector.operate(create, None)
        for type_tag in self.usersroot.findall('row'):
            user_id = type_tag.get('Id')
            user_name = type_tag.get('DisplayName')
            reputation = type_tag.get('Reputation')
            creation_date = type_tag.get('CreationDate')
            last_active_date = type_tag.get('LastAccessDate')
            location = type_tag.get('Location')
            about = type_tag.get('AboutMe')
            if int(user_id) > 500:
                break
            string = "INSERT INTO Users (user_id, user_name, location, reputation, creation_date, last_active_date, about) VALUES (%s, %s, %s, %s, %s, %s, %s);"
            connector.operate(string, (user_id, user_name, location, reputation, datetime.datetime.strptime(creation_date, '%Y-%m-%dT%H:%M:%S.%f'), datetime.datetime.strptime(last_active_date, '%Y-%m-%dT%H:%M:%S.%f'), about))

        print ("Initialization for table Users complete.")

    def buildposts(self, connector, create):
        print ("Initializing table Posts...this may take a few minutes")
        connector.operate(create, None)
        for type_tag in self.postsroot.findall('row'):
            post_id = type_tag.get('Id')
            parent_id = type_tag.get('ParentId')
            creation_date = type_tag.get('CreationDate')
            last_edit_date = type_tag.get('LastEditDate')
            favorite_count = type_tag.get('FavoriteCount')
            view_count = type_tag.get('ViewCount')
            title = type_tag.get('Title')
            body = type_tag.get('Body')
            score = type_tag.get('Score')
            if int(post_id) > 1500:
                break

            if last_edit_date == None:
                last_edit_date = creation_date

            string = "INSERT INTO Posts (post_id, creation_date, last_edit_date, favorite_count, view_count, score, title, body) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"

            if parent_id == None:
                connector.operate(string, (post_id, datetime.datetime.strptime(creation_date, '%Y-%m-%dT%H:%M:%S.%f'), datetime.datetime.strptime(last_edit_date, '%Y-%m-%dT%H:%M:%S.%f'), favorite_count, view_count, score, title, body))

        print ("Initialization for table Posts complete.")

    def buildposted(self, connector, create):
        print ("Initializing table Posted...this may take a few minutes")
        connector.operate(create, None)
        for type_tag in self.postsroot.findall('row'):
            post_id = type_tag.get('Id')
            owner_id = type_tag.get('OwnerUserId')
            parent_id = type_tag.get('ParentId')
            if int(post_id) > 1500:
                break

            string = "INSERT INTO Posted (user_id, post_id) VALUES (%s, %s);"

            if parent_id == None:
                connector.operate(string, (owner_id, post_id))

        print ("Initialization for table Posted complete.")

    def buildtagged(self, connector, create):
        print ("Initializing table Tagged...this may take a few minutes")
        connector.operate(create, None)
        for type_tag in self.postsroot.findall('row'):
            post_id = type_tag.get('Id')
            parent_id = type_tag.get('ParentId')
            tags = type_tag.get('Tags')
            if int(post_id) > 1500:
                break

            if parent_id != None:
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

                    tag_id = connector.operate('SELECT tag_id FROM Tags WHERE tag_name = \'{0}\''.format(tag), None)
                    if tag_id == []:
                        continue
                    tag_id = ((tag_id[0])[0])

                    string = "INSERT INTO Tagged (tag_id, post_id) VALUES (%s, %s)"

                    connector.operate(string, (tag_id, post_id))

        print ("Initialization for table Tagged complete.")

    def buildcomments(self, connector, create):
        print ("Initializing table Comments...this may take a few minutes")
        connector.operate(create, None)
        for type_tag in self.commentsroot.findall('row'):
            comment_id = type_tag.get('Id')
            creation_date = type_tag.get('CreationDate')
            score = type_tag.get('Score')
            text = type_tag.get('Text')
            if int(comment_id) > 1500:
                break

            string = "INSERT INTO Comments (comment_id, score, creation_date, text) VALUES (%s, %s, %s, %s);"

            connector.operate(string, (comment_id, score, datetime.datetime.strptime(creation_date, '%Y-%m-%dT%H:%M:%S.%f'), text))

        print ("Initialization for table Comments complete.")

    def buildcommented(self, connector, create):
        print ("Initializing table Commented...this may take a few minutes")
        connector.operate(create, None)
        for type_tag in self.commentsroot.findall('row'):
            comment_id = type_tag.get('Id')
            user_id = type_tag.get('UserId')
            if int(comment_id) > 1500:
                break

            string = "INSERT INTO Commented (user_id, comment_id) VALUES (%s, %s);"

            connector.operate(string, (user_id, comment_id))

        print ("Initialization for table Commented complete.")

    def buildthread(self, connector, create):
        print ("Initializing table Thread...this may take a few minutes")
        connector.operate(create, None)
        for type_tag in self.commentsroot.findall('row'):
            comment_id = type_tag.get('Id')
            post_id = type_tag.get('PostId')
            if int(comment_id) > 1500:
                break

            string = "INSERT INTO Thread (post_id, comment_id) VALUES (%s, %s);"

            connector.operate(string, (post_id, comment_id))

        print ("Initialization for table Thread complete.")

    def buildbadges(self, connector, create):
        print ("Initializing table Badges...this may take a few minutes")
        connector.operate(create, None)
        for type_tag in self.commentsroot.findall('row'):
            badge_id = type_tag.get('Id')
            badge_name = type_tag.get('PostId')
            if int(badge_id) > 1500:
                break

            string = "INSERT INTO Badges (badge_id, badge_name) VALUES (%s, %s);"

            connector.operate(string, (badge_id, badge_name))

        print ("Initialization for table Badges complete.")

    def builddecorated(self, connector, create):
        print ("Initializing table Decorated...this may take a few minutes")
        connector.operate(create, None)
        for type_tag in self.badgesroot.findall('row'):
            user_id = type_tag.get('UserId')
            badge_id = type_tag.get('Id')
            date_awarded = type_tag.get('Date')
            if int(badge_id) > 1500:
                break

            string = "INSERT INTO Decorated (user_id, badge_id, date_awarded) VALUES (%s, %s, %s);"
  
            #print (user_id, badge_id, datetime.datetime.strptime(date_awarded, '%Y-%m-%dT%H:%M:%S.%f'))

            connector.operate(string, (user_id, badge_id, datetime.datetime.strptime(date_awarded, '%Y-%m-%dT%H:%M:%S.%f')))

        print ("Initialization for table Decorated complete.")

if __name__ == '__main__':
    # create a Connector instance and connect to the database
    connector = Connector()
    connector.connect()

    # entity table creation statements
    createtags = 'CREATE TABLE Tags (tag_id int PRIMARY KEY, tag_name varchar(30))'
    createusers = 'CREATE TABLE Users (user_id int PRIMARY KEY, user_name varchar(50), location varchar(50), reputation int, creation_date timestamp, last_active_date timestamp, about varchar(1000))'
    createposts = 'CREATE TABLE Posts (post_id int PRIMARY KEY, creation_date timestamp, last_edit_date timestamp, favorite_count int, view_count int, score int, title varchar(100), body varchar(5000))'
    createcomments = 'CREATE TABLE Comments (comment_id int PRIMARY KEY, score int, creation_date timestamp, text varchar(5000))'
    createbadges = 'CREATE TABLE Badges (badge_id int PRIMARY KEY, badge_name varchar(100))'

    # relationship table creation statements
    createposted = 'CREATE TABLE Posted (user_id int REFERENCES Users(user_id), post_id int REFERENCES Posts(post_id))'
    createtagged = 'CREATE TABLE Tagged (tag_id int REFERENCES Tags(tag_id), post_id int REFERENCES Posts(post_id))'
    createcommented = 'CREATE TABLE Commented (user_id int REFERENCES Users(user_id), comment_id int REFERENCES Comments(comment_id))'
    createthread = 'CREATE TABLE Thread (post_id int REFERENCES Posts(post_id), comment_id int REFERENCES Comments(comment_id))'
    createdecorated = 'CREATE TABLE Decorated (user_id int REFERENCES Users(user_id), badge_id int REFERENCES Badges(badge_id), date_awarded timestamp)'

    # initialize dbbuilder
    dbbuilder = DBbuilder()
    
    # create entity tables
    dbbuilder.buildtags(connector, 'Tags', createtags)
    dbbuilder.buildusers(connector, createusers)
    dbbuilder.buildposts(connector, createposts)
    dbbuilder.buildcomments(connector, createcomments)
    dbbuilder.buildbadges(connector, createbadges)

    # create relationship tables
    dbbuilder.buildposted(connector, createposted)
    dbbuilder.buildtagged(connector, createtagged)
    dbbuilder.buildcommented(connector, createcommented)
    dbbuilder.buildthread(connector, createthread)
    dbbuilder.builddecorated(connector, createdecorated)

    # disconnect from the database
    connector.disconnect()