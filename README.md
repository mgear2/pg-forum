![pgforumtitle](pg-forum_title.png)

# pg-forum
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# Usage

Python 3.7+
Built to utilize pipenv. If pipenv is not installed, type `pip install pipenv`  
Credentials are held in the database.ini file.  
database.ini should be created/modified accordingly by the user before running the app.  
From the root directory, type `pipenv install`. Any dependencies will be installed.  
From the root directory, type `pipenv run app\dbbuilder.py` to build out the database.  
database and schema names are defined in database.ini  
From the root directory, type `pipenv run app\main.py` to run the app.  
Running either main.py or dbbuilder.py from within the app subdirectory will raise an error.  
  
A more thorough README is coming. In the meantime, see Project_Submission_3.pdf for further details.  
  
# Todo:

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

## License  
  
This program is licensed under the "MIT License".  Please
see the file `LICENSE` in the source distribution of this
software for license terms.  

## Misc

Title Art generated at https://fontmeme.com/pixel-fonts/
