# Challenge  

This application has 3 endpoints 
*/employees POST will recieve a file key=file must be a csv file with 5 rows and the 3rd column is a datetime in ISO format. The process copy the file in the folder employees and upload the data in a table 
*/total_employees_per_quarter GET Will return Number of employees hired for each job and department in 2021 divided by quarter. The table must be ordered alphabetically by department and job.
*/top_hired_by_department GET Will List of ids, name and number of employees hired of each department that hired more employees than the mean of employees hired in 2021 for all the departments, ordered by the number of employees hired (descending).
__Prerequisites:__
* Python 3.7+ or later (Python 3.11+ is preferred)
* A smile on your face

# Setup

## MySQL
* Create a new Mysql server 
* Execute db_scripts files by order. This will create tables, views and the insert for departments and jobs.


# Running
## Set Up Environment Variables
Set the following variables:

```bash
export DB_USER='your-private-db_user'
export DB_PASS='your-private-db_pass"'
export DB_HOST='your-private-db_host"'
export DB_NAME='your-private-db_name'

```

## Installing Dependencies
Install the required Python packages. It's recommended to create a virtual environment first:

```bash
python -m venv env
source env/bin/activate  
```
Then install the packages:

```
pip install -r requirements.txt
```

## Running the Application
Run the application, use:

```commandline
fastapi dev
```

This will start the application, and it will begin listening for messages on the specified port.


# Stopping the Application
To stop the application, use Ctrl+C. The application is set up to handle graceful shutdown and will attempt to process all remaining messages before exiting.

