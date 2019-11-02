# pg-data-archiver
Simple script to archive unused old data from PostgreSQL database tables 

## Setup
```
git clone git@github.com:dunzoit/pg-data-archiver.git
cd pg-data-archiver
pip3 install virtualenv
virtualenv venv
. venv/bin/activate
pip install -r requirements.lock
cp settings.py.sample settings.py
```

Modify `settings.py` to update the connection credentials for your database along with the AWS credentials

 ## Usage

***CAUTION:*** *Run archival on small data chunks to avoid script from consuming too much DB resources especially READ and WRITE IOPS and CPU*

Some test data is available in `test_data.sql`. Feel free to use it to test the below examples.

**Perform a dry run:** Connects to the database, selects the data for archival and uploads the data to s3 and verifies the uploaded file contents. Always default to dryrun.
```
archiver.py -q "select * from employee where id < 3" -d "delete from employee where id < 3" -r dryrun -f "emp_data_0_to_2"
```
```
archiver.py -q "select * from employee where id < 3" -d "delete from employee where id < 3" -f "emp_data_0_to_2"
```

**Archive the data:** Connects to the database, selects the data for archival and uploads the data to s3 and verifies the uploaded file contents. If verification of uploaded data and data selected for archival match, then runs the deletion query and deletes the data from the table.
```
archiver.py -q "select * from employee where id < 3" -d "delete from employee where id < 3" -r archive -f "emp_data_0_to_2"
```

***NOTE:*** The script verifies and expects the filter conditions on `SELECT` and `DELETE` to be same.
