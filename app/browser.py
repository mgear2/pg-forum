class Browser:
    def __init__(self, connector):
        self.connector = connector

    def sqlrunner(self):
        while True:
            userstring = input("Enter Command: ")
            if userstring == "exit": 
                return "exit"
            if userstring == "back":
                return
            returnval = self.connector.operate(userstring, None)
            print(userstring + ' returns:')
            print(type(returnval))
            if(isinstance(returnval, list)): 
                for val in returnval:
                    print(val)
                if(isinstance(returnval, Exception)):
                    print(returnval)

    def exploreusers(self):
        return