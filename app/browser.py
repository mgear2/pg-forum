import sys

class Browser:
    def __init__(self, connector):
        self.connector = connector
        self.exploreusers = "SELECT user_id, user_name, location FROM Users OFFSET (%s) LIMIT (%s)"
        self.exploreposts = "SELECT post_id, title FROM Posts WHERE title != 'None' OFFSET (%s) LIMIT (%s)"
        self.exploretags = "SELECT tag_id, tag_name FROM Tags OFFSET (%s) LIMIT (%s)"
        self.viewuser = "SELECT users.user_id, user_name, location, reputation, creation_date, last_active_date FROM Users, Decorated, Badges WHERE users.user_id = (%s)"
        self.userbadges = "SELECT badge_name FROM Users, Decorated, Badges WHERE users.user_id = (%s) AND decorated.user_id=(%s) AND badges.badge_id=decorated.badge_id"
        self.viewpost = "SELECT creation_date, last_edit_date, favorite_count, view_count, score, title FROM Posts WHERE post_id = (%s)"
        self.viewtag = "SELECT tag_id, tag_name FROM Tags WHERE tag_id = (%s)"
        self.offset = 0
        self.limit = 10

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
            string = self.exploreusers
        elif context == "posts":
            string = self.exploreposts
        elif context == "tags":
            string = self.exploretags
        else:
            print ("Can't explore {0}".format(context))
            return

        userstring = ""

        while True:
            if userstring == "":
                returnval = self.connector.operate(string, (self.offset, self.limit))
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

    def printpost(self, rows):
        for row in rows:
            print("Title:\t{0}".format(row[5]))
            print("By:\tScore: {0}\tViews: {1}\tFavorites: {2}".format(row[4], row[3], row[2]))
            print("Posted: {0}\tLast Edited: {1}".format(row[0], row[1]))

        "SELECT creation_date, last_edit_date, favorite_count, view_count, score, title FROM Posts WHERE post_id = (%s)"

    def view(self, context, given_id):
        print("Viewing {0} with defining attribute {1}".format(context, given_id))
        infostring = "<ENTER> for more results, \'back\' to return to command line"
        self.offset = 0

        if context == "user":
            string = self.viewuser
        elif context == "post":
            print(infostring)
            string = self.viewpost
        elif context == "tag":
            print(infostring)
            string = self.viewtag
        else:
            print ("Can't view {0}".format(context))
            return

        userstring = ""

        while True:
            if userstring == "":
                returnval = self.connector.operate(string, given_id)
                if(isinstance(returnval, list)):                
                    if returnval == []: 
                        print ("End of results")
                        break
                    elif context == "user":
                        badges = "Test Badges"
                        self.printuser(returnval[0], badges)
                        break
                    elif context == "post":
                        self.printpost(returnval)
                        break
                userstring = input("<ENTER>/\'back\': ")
                self.offset += 10
                continue
            if userstring == "exit":
                self.exit()
            if userstring == "back":
                print("Exiting explorer")
                return