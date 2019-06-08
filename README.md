# postgresApp
INSTRUCTIONS FOR USE: 
Built for python 3.7
Built to utilize pipenv. If pipenv is not installed, type "pip install pipenv". 
Credentials are held in the database.ini file. This file should be created/modified accordingly by the user before running the app. 
From the root directory, type "pipenv install". Any dependencies will be installed.
From the root directory, type "pipenv run app\dbbuilder.py" to build out the database (database and schema names defined in database.ini). Running from within the app subdirectory will raise an error.  
From the root directory, type "pipenv run app\main.py" to run the app. Running from within the app subdirectory will raise an error. 

More thorough README is coming. In the meantime, see Project_Submission_3.pdf for further details.

TODO:
Add tags to post printouts
Sanizite inputs for sql queries for inserting tags on new post creation
Add thorough unit tests
Add docstrings
Add more comments
Complete implementation of user permissions/roles
Implement ability for admins to:
    ban users, lock/unlock threads, promote/demote moderators
Implement printing functionality for subcomments
Implement ability to create subcomments