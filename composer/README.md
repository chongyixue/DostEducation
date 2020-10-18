# Google Cloud Storage and Database
1. Download these files onto a folder in google storage, noting down the file path to the folder, eg. `gs://clusterjob`
	- create3tables.py
	- mainfact.py
	- forHMLagg.py
	- chartHML.py
	- postgresql-42.2.16.jar

2. Set up a google-managed postgreSQL database under the same project as your database. This new database will be the warehouse. Make sure to enable private IP in both the database and warehouse.

# Setting up Google Composer
1. On GCP, select Composer then select create. Change the location to the that of your database and choose a name. Other options can be detault. It will take 5-15 minutes to create the environment.

2. Once created, click on the Airflow link to bring up the Airflow UI.Under Admin, select Variables to create and save the key-value pairs of keys:
	- jarpath
	- readuser (postgreSQL database username)
	- readpassword 
	- writeuser (postgreSQL datawarehouse username)
	- writepassword
	- readIP (internal IP of database)
	- writeIP (internal IP of warehouse)
	- dataproc\_zone
	- project\_id

3. On the Composer Environment page, click on the DAG folder then upload `dostdagdemo.py` into the folder.

4. On the airflow page click on the dag that was submitted to check status, using the graph view.

# Running a job on Dataproc Cluster manually (optional)
The same python codes can be run from a Dataproc cluster directly, and it is useful for testing the code before being inserted into the Airflow pipeline. For each job submission
	- dataproc submit job setting: pyspark
	- Main python file: provide the path to python file eg. `gs://clusterjob/simplejob.py`
	- jar : <path to jar>
	- Additional arguments (a-g order is important corresponding to step 2 in the previous section)
		a. readuser
		b. readpassword
		c. writeuser
		d. writepassword
		e. readIP
		f. writeIP
		g. jarpath
		h. --driver-class-path <jarpath>
		i. --jars <jarpath>



















