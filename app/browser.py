import sys
from textwrap import TextWrapper

class Browser:
    def __init__(self, connector):
        self.connector = connector
        self.exploreusers = "SELECT user_id, user_name, location FROM Users OFFSET (%s) LIMIT (%s)"
        self.exploreposts = "SELECT post_id, title FROM Posts WHERE title != 'None' OFFSET (%s) LIMIT (%s)"
        self.exploretags = "SELECT tag_id, tag_name FROM Tags OFFSET (%s) LIMIT (%s)"
        self.viewuser = "SELECT users.user_id, user_name, location, reputation, creation_date, last_active_date FROM Users WHERE users.user_id = (%s)"
        self.userbadges = "SELECT badge_name FROM Users, Decorated, Badges WHERE users.user_id = (%s) AND decorated.user_id=(%s) AND badges.badge_id=decorated.badge_id"
        self.viewpost = "SELECT creation_date, last_edit_date, favorite_count, view_count, score, title, post_id, body FROM posts WHERE post_id = (%s)"
        self.viewposter = "SELECT users.user_name FROM users, posted WHERE posted.post_id = (%s) AND users.user_id=posted.user_id"
        self.viewsubposts = "SELECT creation_date, last_edit_date, favorite_count, view_count, score, title, posts.post_id, body FROM posts, subposts WHERE subposts.parent_id = (%s) AND Posts.post_id = Subposts.child_id"
        self.viewcomments = """SELECT thread.post_id, comments.comment_id, comments.score, comments.creation_date, comments.text 
                            FROM Comments, Thread, Posts WHERE posts.post_id = (%s) AND posts.post_id = thread.post_id AND thread.comment_id = comments.comment_id 
                            """
        self.viewtag = "SELECT tag_id, tag_name FROM Tags WHERE tag_id = (%s)"
        self.offset = 0
        self.limit = 10
        self.divider = "--------------------------------------------------------------------------------------"

    def exit(self):
        self.connector.disconnect()
        sys.exit(0)

    def commandrunner(self):
        while True:
            userstring = input("Enter Command: ")

            if userstring == "":
                continue
            if userstring == "exit": 
                self.exit()
            if userstring == "query tool":
                self.sqlrunner()
                continue
            if userstring.split(' ')[0] == "explore":
                if len(userstring.split(' ')) < 2:
                    print("Please define a context to explore")
                    continue
                self.explore(userstring.split(' ')[1])
                continue
            if userstring.split(' ')[0] == "view":
                if len(userstring.split(' ')) < 3:
                    print("Please define both a context and an id to view")
                    continue
                self.view(userstring.split(' ')[1], userstring.split(' ')[2])
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
            print(userstring + ' returns:')
            print(type(returnval))
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
            print ("Can't explore {0}".format(context))
            return

        userstring = ""

        while True:
            if userstring == "":
                returnval = self.connector.operate(inputstring, (self.offset, self.limit))
                if(isinstance(returnval, list)): 
                    for val in returnval:
                        print(val)                    
                    if returnval == []: 
                        print ("End of results")
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
        for i in range(0,indent):
            indentstring += "\t"
        wrapper = TextWrapper(width=150, initial_indent=indentstring, subsequent_indent=indentstring)
        #for row in rows:
        if indent == 0:
            print(self.divider + "\n" + self.divider)
            print("{0}Title:\t{1}".format(indentstring, row[5]))
        body = wrapper.wrap(row[7])
        for line in body:
            print (line)
        print("{0}By:\t{1}\t\tScore: {2}\tViews: {3}\tFavorites: {4}".format(indentstring, postuser, row[4], row[3], row[2]))
        print("{0}Posted: {1}\tLast Edited: {2}".format(indentstring, row[0], row[1]))
        print(self.divider)

    def printcomments(self, comments, indent):
        for row in comments:
            indentstring = ""
            for i in range(0,indent):
                indentstring += "\t"
            wrapper = TextWrapper(width=150, initial_indent=indentstring, subsequent_indent=indentstring)
            body = wrapper.wrap(row[4])
            for line in body:
                print (line)
            print("{0}By:\t{1}\t\tScore: {2}".format(indentstring, "TEST", row[2]))
            print("{0}Posted: {1}".format(indentstring, row[3]))
            print(self.divider)

        "thread.post_id, comments.comment_id, comments.score, comments.creation_date, comments.text "

    def view(self, context, given_id):
        print("Viewing {0} with ID {1}".format(context, given_id))
        #infostring = "<ENTER> for more results, \'back\' to return to command line"
        self.offset = 0

        if context == "user":
            inputstring = self.viewuser
        elif context == "post":
            #print(infostring)
            inputstring = self.viewpost
        elif context == "tag":
            #print(infostring)
            inputstring = self.viewtag
        else:
            print ("Can't view {0}".format(context))
            return

        userstring = ""

        print ("test")  

        while True:
            if userstring == "":
                returnval = self.connector.operate(inputstring, (given_id,))
                if(isinstance(returnval, list)):       
                    if returnval == []: 
                        print ("End of results")
                        break
                    if context == "user":
                        userbadges = self.connector.operate(self.userbadges, (given_id, given_id))
                        badges = []
                        for badge in userbadges:
                            badges += badge
                        self.printuser(returnval[0], badges)
                        return
                    elif context == "post":
                        subposts = self.connector.operate(self.viewsubposts, given_id)
                        postuser = self.connector.operate(self.viewposter, (returnval[0][6],))
                        if postuser == []:
                            postuser = "User not found with Id {0}".format(given_id)
                        else:
                            postuser = postuser[0][0]
                        self.printpost(returnval[0], postuser, 0)
                        for post in subposts:
                            subpostuser = self.connector.operate(self.viewposter, (post[6],))
                            if subpostuser == []:
                                subpostuser = "User not found"
                            else:
                                subpostuser = subpostuser[0][0]
                            self.printpost(post, subpostuser, 1)
                            comments = self.connector.operate(self.viewcomments, (post[6],))
                            self.printcomments(comments, 2)
                        break
                    elif context == "tag":
                        print("Still needs to be implemented...")
                        break
                userstring = input("<ENTER>/\'back\': ")
                self.offset += 10
                continue
            if userstring == "exit":
                self.exit()
            if userstring == "back":
                print("Exiting explorer")
                return

        """if userstring == "":
            returnval = self.connector.operate(inputstring, given_id)
            print(returnval)
            if(isinstance(returnval, list)):       
                if returnval == []: 
                    print ("End of results")
                    return
                if context == "user":
                    userbadges = self.connector.operate(self.userbadges, (given_id, given_id))
                    badges = []
                    for badge in userbadges:
                        badges += badge
                    self.printuser(returnval[0], badges)
                    return
                elif context == "post":
                    subposts = self.connector.operate(self.viewsubposts, given_id)
                    postuser = self.connector.operate(self.viewposter, (returnval[0][6],))
                    if postuser == []:
                        postuser = "User not found with Id {0}".format(given_id)
                    else:
                        postuser = postuser[0][0]
                        print(postuser)
                    self.printpost(returnval[0], postuser, 0)
                    for post in subposts:
                        subpostuser = self.connector.operate(self.viewposter, (post[6],))
                        if subpostuser == []:
                            subpostuser = "User not found"
                        else:
                            subpostuser = subpostuser[0][0]
                        self.printpost(post, subpostuser, 1)
                        comments = self.connector.operate(self.viewcomments, (post[6],))
                        self.printcomments(comments, 2)
                elif context == "tag":
                    print("Still needs to be implemented...")
        if userstring == "exit":
            self.exit()
        if userstring == "back":
            print("Exiting explorer")
            return"""