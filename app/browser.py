class Browser:
    def __init__(self, connector):
        self.connector = connector
        self.exploreusers = "SELECT user_id, user_name, location FROM Users OFFSET (%s) LIMIT (%s)"
        self.offset = 0
        self.limit = 10

    def commandrunner(self):
        while True:
            userstring = input("Enter Command: ")

            if userstring == "":
                continue
            if userstring == "exit": 
                break
            if userstring == "sqlrunner()":
                runnerval = self.sqlrunner()
                if runnerval == "exit":
                    break
                else:
                    continue
            if userstring.split(' ')[0] == "explore":
                if len(userstring.split(' ')) < 2:
                    print("Please define a context to explore")
                    continue
                runnerval = self.explore(userstring.split(' ')[1])
                if runnerval == "exit":
                    break
                else: 
                    continue
            else:
                print("Command not recognized")

    def sqlrunner(self):
        print("Entering sqlrunner()")
        while True:
            userstring = input("Enter SQL Query: ")
            if userstring == "exit": 
                return "exit"
            if userstring == "back":
                print("Exiting sqlrunner()")
                return
            returnval = self.connector.operate(userstring, None)
            print(userstring + ' returns:')
            print(type(returnval))
            if(isinstance(returnval, list)): 
                for val in returnval:
                    print(val)

    def explore(self, context):
        print("Exploring {0}".format(context))
        print("ENTER Key for more results, \'back\' to return to command line")
        self.offset = 0
        if context == "users":
            string = self.exploreusers
        else:
            print ("Can't explore {0}".format(context))
            return
        while True:
            userstring = input("<ENTER>/\'back\': ")
            if userstring == "":
                returnval = self.connector.operate(string, (self.offset, self.limit))
                if(isinstance(returnval, list)): 
                    for val in returnval:
                        print(val)                    
                    if returnval == []: 
                        print ("End of results")
                        break
                self.offset += 10
                continue
            if userstring == "exit":
                return "exit"
            if userstring == "back":
                return