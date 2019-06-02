# pylint: disable=import-error
import xml.etree.ElementTree as ET
from connector import Connector
import psycopg2
import datetime 

class Dataparser():
    def __init__(self):
        self.tags = ET.parse('app/data/woodworking.stackexchange.com/Tags.xml').getroot()
        self.users = ET.parse('app/data/woodworking.stackexchange.com/Users.xml').getroot()
        self.posts = ET.parse('app/data/woodworking.stackexchange.com/Posts.xml').getroot()
        self.comments = ET.parse('app/data/woodworking.stackexchange.com/Comments.xml').getroot()
        self.votes = ET.parse('app/data/woodworking.stackexchange.com/Votes.xml').getroot()
        self.badges = ET.parse('app/data/woodworking.stackexchange.com/Badges.xml').getroot()

    def parsetags(self, connector, table, create):
        print(self.tags.tag)
        connector.operate(create)
        for type_tag in self.tags.findall('row'):
            tid = type_tag.get('Id')
            tname = type_tag.get('TagName')

            string = '{},\'{}\''.format(tid, tname)

            print('Sending: ' + string)
            connector.operate('INSERT INTO {0} VALUES ({1})'.format(table, string))

    def parseusers(self, connector, create):
        print(self.users.tag)
        connector.operate(create)
        for type_tag in self.users.findall('row'):
            user_id = type_tag.get('Id')
            user_name = type_tag.get('DisplayName')
            reputation = type_tag.get('Reputation')
            creation_date = type_tag.get('CreationDate')
            last_active_date = type_tag.get('LastAccessDate')
            location = type_tag.get('Location')
            about = type_tag.get('AboutMe')
            if int(user_id) > 500:
                break

            #string = "{0},\'{1}\',\'{2}\',\'{3}\',{4},\'{5}\',\'{6}\'".format(user_id, user_name, location, about, reputation, datetime.datetime.strptime(creation_date, '%Y-%m-%dT%H:%M:%S.%f'), datetime.datetime.strptime(last_active_date, '%Y-%m-%dT%H:%M:%S.%f'))
            #print('Sending: ' + string)

            string = "INSERT INTO Users (user_id, user_name, location, reputation, creation_date, last_active_date, about) Values (%s, %s, %s, %s, %s, %s, %s);"

            try: 
                connector.cursor.execute(string, (user_id, user_name, location, reputation, datetime.datetime.strptime(creation_date, '%Y-%m-%dT%H:%M:%S.%f'), datetime.datetime.strptime(last_active_date, '%Y-%m-%dT%H:%M:%S.%f'), about))
                connector.connection.commit()
                #returnval = connector.cursor.fetchall()
                #for val in returnval:
                #    print(val)
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)
                connector.connection.rollback()

            #connector.operate('INSERT INTO Users VALUES ({0})'.format(string))

    def parsevotes(self, connector, create):
        return

    def parseposts(self, connector, create):
        print(self.posts.tag)
        connector.operate(create)
        for type_tag in self.posts.findall('row'):
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

            string = "INSERT INTO Posts (post_id, creation_date, last_edit_date, favorite_count, view_count, score, title, body) Values (%s, %s, %s, %s, %s, %s, %s, %s);"

            if parent_id != None:
                try: 
                    connector.cursor.execute(string, (post_id, datetime.datetime.strptime(creation_date, '%Y-%m-%dT%H:%M:%S.%f'), datetime.datetime.strptime(last_edit_date, '%Y-%m-%dT%H:%M:%S.%f'), favorite_count, view_count, score, title, body))
                    connector.connection.commit()
                    #returnval = connector.cursor.fetchall()
                    #for val in returnval:
                    #    print(val)
                except (Exception, psycopg2.DatabaseError) as error:
                    print(error)
                    connector.connection.rollback()

    def parseposted(self, connector, create):
        connector.operate(create)
        for type_tag in self.posts.findall('row'):
            post_id = type_tag.get('Id')
            owner_id = type_tag.get('OwnerUserId')
            parent_id = type_tag.get('ParentId')
            if int(post_id) > 1500:
                break

            string = "INSERT INTO Posted (user_id, post_id) Values (%s, %s);"

            #print(owner_id, post_id, parent_id)

            if parent_id != None:
                try: 
                    connector.cursor.execute(string, (owner_id, post_id))
                    connector.connection.commit()
                    #returnval = connector.cursor.fetchall()
                    #for val in returnval:
                    #    print(val)
                except (Exception, psycopg2.DatabaseError) as error:
                    print(error)
                    connector.connection.rollback()

    def parsetagged(self, connector, create):
        connector.operate(create)
        for type_tag in self.posts.findall('row'):
            post_id = type_tag.get('Id')
            parent_id = type_tag.get('ParentId')
            tags = type_tag.get('Tags')
            if int(post_id) > 1500:
                break

            if parent_id != None:
                continue 

            #print(tags)
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
                    #print(tag)

                    tag_id = connector.operate('SELECT tag_id FROM Tags WHERE tag_name = \'{0}\''.format(tag))
                    print(tag_id)

                    string = "INSERT INTO Tagged (tag_id, post_id) VALUES (%s, %s)"

                    """try: 
                        connector.cursor.execute(string, (tag_id, post_id))
                        connector.connection.commit()
                        #returnval = connector.cursor.fetchall()
                        #for val in returnval:
                        #    print(val)
                    except (Exception, psycopg2.DatabaseError) as error:
                        print(error)
                        connector.connection.rollback()"""

    def parsecomments(self, connector, create):
        return

    def parsebadges(self, connector, create):
        return

if __name__ == '__main__':
    # create a Connector instance and connect to the database
    connector = Connector()
    connector.connect()

    createtags = 'CREATE TABLE Tags (tag_id int PRIMARY KEY, tag_name varchar(30))'
    createusers = 'CREATE TABLE Users (user_id int PRIMARY KEY, user_name varchar(50), location varchar(50), reputation int, creation_date timestamp, last_active_date timestamp, about varchar(1000))'
    createposts = 'CREATE TABLE Posts (post_id int PRIMARY KEY, creation_date timestamp, last_edit_date timestamp, favorite_count int, view_count int, score int, title varchar(100), body varchar(5000))'

    createposted = 'CREATE TABLE Posted (user_id int REFERENCES Users(user_id), post_id int REFERENCES Posts(post_id))'
    createtagged = 'CREATE TABLE Tagged (tag_id int REFERENCES Tags(tag_id), post_id int REFERENCES Posts(Post_id))'

    dataparser = Dataparser()
    #dataparser.parsetags(connector, 'Tags', createtags)
    #dataparser.parseusers(connector, createusers)
    #dataparser.parseposts(connector, createposts)
    #dataparser.parseposted(connector, createposted)
    dataparser.parsetagged(connector, createtagged)
    # disconnect from the database
    connector.disconnect()