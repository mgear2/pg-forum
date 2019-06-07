# pylint: disable=import-error
from textwrap import TextWrapper
import sys
import datetime
import psycopg2

class Browser:
    def __init__(self, connector):
        self.connector = connector
        self.exploreusers = ("SELECT user_id, user_name, location "
            "FROM Users OFFSET (%s) LIMIT (%s)")
        self.exploreposts = ("SELECT post_id, title "
            "FROM Posts WHERE title != 'None' OFFSET (%s) LIMIT (%s)")
        self.exploretags = ("SELECT tag_id, tag_name "
            "FROM Tags OFFSET (%s) LIMIT (%s)")
        self.viewuser = ("SELECT users.user_id, user_name, location,"
            "reputation, creation_date, last_active_date "
            "FROM Users WHERE users.user_id = (%s)")
        self.userbadges = ("SELECT badge_name "
            "FROM Users, Decorated, Badges WHERE users.user_id = (%s) "
            "AND decorated.user_id=(%s) "
            "AND badges.badge_id=decorated.badge_id")
        self.viewpost = ("SELECT creation_date, last_edit_date,"
            "favorite_count, view_count, score, title, post_id, body "
            "FROM posts WHERE post_id = (%s)")
        self.viewposter = ("SELECT users.user_name "
            "FROM users, posted "
            "WHERE posted.post_id = (%s) "
            "AND users.user_id=posted.user_id")
        self.viewsubposts = ("SELECT creation_date, last_edit_date, "
            "favorite_count, view_count, score, title, posts.post_id, body "
            "FROM posts, subposts WHERE subposts.parent_id = (%s) "
            "AND Posts.post_id = Subposts.child_id")
        self.findparent = ("SELECT subposts.parent_id "
            "FROM Subposts WHERE subposts.child_id = (%s)")
        self.viewcomments = ("SELECT thread.post_id, comments.comment_id,"
            "comments.score, comments.creation_date, comments.text "
            "FROM Comments, Thread, Posts "
            "WHERE posts.post_id = (%s) "
            "AND posts.post_id = thread.post_id "
            "AND thread.comment_id = comments.comment_id")
        self.viewcommenter = ("SELECT users.user_name "
            "FROM Commented, Users "
            "WHERE commented.comment_id = (%s) "
            "AND commented.user_id = users.user_id")
        self.confirmtag = ("SELECT tags.tag_name, posts.post_id, posts.title "
            "FROM Tags, Posts, Tagged WHERE tags.tag_id = (%s) "
            "AND tags.tag_id = tagged.tag_id "
            "AND tagged.post_id = posts.post_id LIMIT 5")
        self.viewtagposts = ("SELECT tags.tag_name, posts.post_id, posts.title "
            "FROM Tags, Posts, Tagged WHERE tags.tag_id = (%s) "
            "AND tags.tag_id = tagged.tag_id "
            "AND tagged.post_id = posts.post_id OFFSET (%s) LIMIT (%s)")
        self._newpostid = "SELECT max(post_id) FROM Posts"
        self._newposted = ("INSERT INTO Posted (user_id, post_id) "
            "VALUES (%s, %s)")
        self._newpost = "CALL newpost(%s, %s, %s, %s, %s, %s, %s, %s)"
        self._posttag = "INSERT INTO Tagged (tag_id, post_id) VALUES (%s, %s)"
        self._findtagid = ("SELECT tag_id, tag_name "
            "FROM Tags WHERE tag_name = (%s)")
        self._newsubpost = ("INSERT INTO Subposts (parent_id, child_id) "
            "VALUES (%s, %s)")
        self._newcomment = ("INSERT INTO Comments "
            "(comment_id, score, creation_date, text) "
            "VALUES (%s, %s, %s, %s)")
        self._newcommentid = "SELECT max(comment_id) FROM Comments"
        self._newcommented = ("INSERT INTO Commented "
            "(user_id, comment_id) VALUES (%s, %s)")
        self._newthread = ("INSERT INTO Thread "
            "(post_id, comment_id) VALUES (%s, %s)")
        self._deletefromtagged = ("DELETE FROM Tagged "
            "WHERE Tagged.post_id = (%s)")
        self._deletefromposts = ("DELETE FROM Posts "
            "WHERE Posts.post_id=(%s)")
        self._deletefromsubpost = ("DELETE FROM Subposts "
            "WHERE parent_id = (%s) OR child_id = (%s)")
        self._deletefromposted = ("DELETE FROM Posted "
            "WHERE user_id = (%s) OR post_id = (%s)")
        self._deletefromcommented = ("DELETE FROM Commented "
            "WHERE comment_id = (%s)")
        self._deletefromcomments = ("DELETE FROM Comments "
            "WHERE comment_id = (%s)")
        self._deletefromthread = ("DELETE FROM Thread "
            "WHERE comment_id = (%s) OR post_id = (%s)")
        self.id = -999
        self.offset = 0
        self.limit = 10
        self.divider = ("----------------------------------"
            "---------------------------------------")

    def exit(self):
        self.connector.disconnect()
        sys.exit(0)

    def commandrunner(self):
        while True:
            userstring = input("Enter Command: ")
            verify = userstring.split(' ')

            if userstring == "":
                continue
            elif userstring == "exit":
                self.exit()
            elif (userstring == "query tool" 
                    or userstring == "sqlrunner"):
                self.sqlrunner()
                continue
            elif verify[0] == "explore":
                if len(verify) < 2:
                    print("Please define a context to explore")
                    continue
                self.explore(verify[1])
                continue
            elif verify[0] == "view":
                if len(verify) < 3:
                    print("Please define both a context and an id to view")
                    continue
                self.view(verify[1], verify[2])
                continue
            elif verify[0] == "new":
                if len(verify) < 2:
                    print("Please define a context for new")
                    continue
                self.new(verify)
                continue
            elif verify[0] == "delete":
                if len(verify) < 2:
                    print("Please define a context for delete")
                    continue
                self.delete(verify)
                continue
            else:
                print("Command not recognized")

    def sqlrunner(self):
        print("Entering Query Tool")

        while True:
            userstring = input("Enter SQL Query: ")
            if userstring == "exit":
                self.exit()
            if userstring == "back":
                print("Exiting Query Tool")
                return
            returnval = self.connector.operate(userstring, None)
            # print(userstring + ' returns:')
            # print(type(returnval))
            if(isinstance(returnval, list)):
                for val in returnval:
                    print(val)

    def explore(self, context):
        print("Exploring {0}".format(context))
        print("<ENTER> for more results, \'back\' to return to command line")
        self.offset = 0

        if context == "users":
            inputstring = self.exploreusers
        elif context == "posts":
            inputstring = self.exploreposts
        elif context == "tags":
            inputstring = self.exploretags
        else:
            print("Can't explore {0}".format(context))
            return

        userstring = ""

        while True:
            if userstring == "":
                returnval = self.connector.operate(
                    inputstring, (self.offset, self.limit))
                if(isinstance(returnval, list)):
                    for val in returnval:
                        print(val)
                    if returnval == [] or len(returnval) < 10:
                        print("End of results")
                        break
                userstring = input("<ENTER>/\'back\': ")
                self.offset += 10
                continue
            if userstring == "exit":
                self.exit()
            if userstring == "back":
                print("Exiting explorer")
                return

    def printuser(self, row, badges):
        print("Id:\t\t{0}".format(row[0]))
        print("Name:\t\t{0}".format(row[1]))
        print("Location:\t{0}".format(row[2]))
        print("Badges:\t\t{0}".format(badges))
        print("Reputation:\t{0}".format(row[3]))
        print("Joined:\t\t{0}".format(row[4]))
        print("Last Active:\t{0}".format(row[5]))

    def printpost(self, row, postuser, indent):
        indentstring = ""
        title = row[5]
        for i in range(0, indent):
            indentstring += "\t"
        wrapper = TextWrapper(
            width=79, initial_indent=indentstring, 
            subsequent_indent=indentstring)
        if row[5] == None and indent == 0:
            parent = self.connector.operate(self.findparent, (row[6],))
            if parent != []:
                title = "Subpost of Post {0}".format(parent[0][0])
        if indent == 0:
            print(self.divider + "\n" + self.divider)
            print("{0}Title:\t{1}".format(indentstring, title))
        body = wrapper.wrap(row[7])
        for line in body:
            print(line)
        print(("{0}By:\t{1}\tID: {5}\t\tScore: {2}\t"
            "Views: {3}\tFavorites: {4}".format(
            indentstring, postuser, row[4], row[3], row[2], row[6])))
        print("{0}Posted: {1}\tLast Edited: {2}".format(
            indentstring, row[0], row[1]))
        print(self.divider)

    def printcomments(self, comments, indent):
        for row in comments:
            commentuser = self.connector.operate(self.viewcommenter, (row[1],))
            if (isinstance(commentuser, list) == False 
                    or commentuser == []):
                commentuser = "Not found"
            else:
                commentuser = commentuser[0][0]
            indentstring = ""
            for i in range(0, indent):
                indentstring += "\t"
            wrapper = TextWrapper(
                width=79, initial_indent=indentstring, 
                subsequent_indent=indentstring)
            body = wrapper.wrap(row[4])
            for line in body:
                print(line)
            print("{0}By:\t{1}\tID: {2}\t\tScore: {3}".format(
                indentstring, commentuser, row[1], row[2]))
            print("{0}Posted: {1}".format(indentstring, row[3]))
            print(self.divider)

    def view(self, context, given_id):
        print("Viewing {0} with ID {1}".format(context, given_id))
        self.offset = 0

        if context == "user":
            inputstring = self.viewuser
        elif context == "post":
            inputstring = self.viewpost
        elif context == "tag":
            inputstring = self.confirmtag
        else:
            print("Can't view {0}".format(context))
            return

        returnval = self.connector.operate(inputstring, (given_id,))
        if(isinstance(returnval, list)):
            if returnval == []:
                print("No results")
                return
            if context == "user":
                userbadges = self.connector.operate(
                    self.userbadges, (given_id, given_id))
                badges = []
                for badge in userbadges:
                    badges += badge
                self.printuser(returnval[0], badges)
                return
            elif context == "post":
                subposts = self.connector.operate(
                    self.viewsubposts, (given_id,))
                postuser = self.connector.operate(
                    self.viewposter, (returnval[0][6],))
                if postuser == []:
                    postuser = "User not found with Id {0}".format(
                        given_id)
                else:
                    postuser = postuser[0][0]
                self.printpost(returnval[0], postuser, 0)
                comments = (self.connector.operate(
                    self.viewcomments, (given_id,)))
                self.printcomments(comments, 2)
                for post in subposts:
                    subpostuser = self.connector.operate(
                        self.viewposter, (post[6],))
                    if subpostuser == []:
                        subpostuser = "User not found"
                    else:
                        subpostuser = subpostuser[0][0]
                    self.printpost(post, subpostuser, 1)
                    comments = self.connector.operate(
                        self.viewcomments, (post[6],))
                    self.printcomments(comments, 2)
                return
            elif context == "tag":
                returnval = self.connector.operate(
                    self.viewtagposts, 
                    (given_id, self.offset, self.limit))
                if(isinstance(returnval, list)):
                    for val in returnval:
                        print(val)
                    if returnval == [] or len(returnval) < 10:
                        print("End of results")
                        return

    def new(self, verifylist):
        if verifylist[1] == "post":
            if len(verifylist) < 3:
                self.newpost(verifylist)
                return
            else:
                self.newsubpost(verifylist)
                return
        if verifylist[1] == "comment":
            if len(verifylist) < 3:
                print("Please define a post id to comment on")
                return
            else:
                self.newcomment(verifylist)
                return
        else:
            print("Unrecognized command")
            return

    def newpost(self, verifylist):
        newtitle = ""
        newtitle = input("Enter Post Title: ")
        now = datetime.datetime.now()
        newbody = ""
        newbody = input("Enter Post Body: ")
        newtags = input("Enter Post Tags as <Tag1,Tag2,Tag3...>: ")
        newtags = newtags.split(',')
        newid = self.connector.operate(self._newpostid, None)
        newid = newid[0][0]
        newid += 1
        newpost = (newid, now, now, 0, 0, 0, newtitle, newbody)
        self.connector.operate(self._newpost, newpost)
        self.connector.operate(self._newposted, (self.id, newid))
        string = "INSERT INTO Tagged (tag_id, post_id) VALUES "
        tuples = ()
        i = 0
        for tag in newtags:
            i += 1
            dbtag = self.connector.operate(self._findtagid, (tag,))
            string += "(%s, %s)"
            tuples += dbtag[0][0],newid
            if i < len(newtags):
                string += ","
        self.connector.operate(string, tuples)
        print("Created new post with ID {0}".format(newid))

    def newsubpost(self, verifylist):
        newtitle = None
        parent = self.connector.operate(self.viewpost, (verifylist[2],))
        if parent == []:
            print("Given Post ID not found")
            return
        if isinstance(parent, psycopg2.errors.InvalidTextRepresentation):
            print("ID must be integer")
            return
        parent = parent[0][6]
        now = datetime.datetime.now()
        newbody = ""
        newbody = input("Enter Post Body: ")
        newid = self.connector.operate(self._newpostid, None)
        newid = newid[0][0]
        newid += 1
        newpost = (newid, now, now, 0, 0, 0, newtitle, newbody)
        self.connector.operate(self._newpost, newpost)
        self.connector.operate(self._newposted, (self.id, newid))
        self.connector.operate(self._newsubpost, (parent, newid))
        print("Created new post with ID {0} on Parent {1}".format(newid, parent))

    def newcomment(self, verifylist):
        parent = verifylist[2]
        if parent == []:
            print("Given Post ID not found")
            return
        if isinstance(parent, psycopg2.errors.InvalidTextRepresentation):
            print("ID must be integer")
            return
        print(parent)
        now = datetime.datetime.now()
        newbody = input("Enter Post Body: ")
        newid = self.connector.operate(self._newcommentid, None)
        newid = newid[0][0]
        newid += 1
        newcomment = (newid, 0, now, newbody)
        self.connector.operate(self._newcomment, newcomment)
        self.connector.operate(self._newcommented, (self.id, newid))
        self.connector.operate(self._newthread, (parent, newid))
        print("Created new Comment with ID {0} on Parent {1}".format(newid, parent))

    def delete(self, verifylist):
        if len(verifylist) < 3:
                print("Please define an ID to delete")
                return
        if verifylist[1] == "post":
            self.deletepost(verifylist)
            return
        if verifylist[1] == "comment":
            self.deletecomment(verifylist)
            return
        else:
            print("Unrecognized command")
            return

    def deletepost(self, verifylist):
        postid = verifylist[2]
        parent = self.connector.operate(self.viewpost, (verifylist[2],))
        if parent != []:
            parent = parent[0][6]
            self.connector.operate(self._deletefromsubpost, (postid, parent))
        self.connector.operate(self._deletefromposted, (self.id, postid))
        self.connector.operate(self._deletefromtagged, (postid,))
        self.connector.operate(self._deletefromposts, (postid,))
        print("Deleted post with ID {0}".format(postid))

    def deletecomment(self, verifylist):
        commentid = verifylist[2]
        self.connector.operate(self._deletefromcommented, (commentid,))
        self.connector.operate(self._deletefromthread, (commentid, self.id))
        self.connector.operate(self._deletefromcomments, (commentid,))
        print("Deleted comment with ID {0}".format(commentid))