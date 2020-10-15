# STEP 1: Libraries needed
from datetime import timedelta, datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule
#from airflow import DAG


# STEP 2:Define a start date
#In this case yesterday
yesterday = datetime(2020, 10, 13)

SPARK_CODE0=('gs://dostbucket/clusterjob/create3tables.py')
SPARK_CODE1=('gs://dostbucket/clusterjob/createmain.py')
SPARK_CODE2=('gs://dostbucket/clusterjob/forHMLagg.py')
SPARK_CODE3=('gs://dostbucket/clusterjob/chartHML.py')

dataproc_job_name='spark_job_dataproc'

#jarpath='gs://dostbucket/jar/postgresql-42.2.16.jar'
#readuser='postgres'
#readpassword='Z866c269x'
#writeuser='postgres'
#writepassword='Z866c269x'
#readIP='10.23.240.3'
#writeIP='10.23.240.5'

jarpath=models.Variable.get('jarpath')
readuser=models.Variable.get('readuser')
readpassword=models.Variable.get('readpassword')
writeuser=models.Variable.get('writeuser')
writepassword=models.Variable.get('writepassword')
readIP=models.Variable.get('readIP')
writeIP=models.Variable.get('writeIP')

additionalarg1='--driver-class-path '+jarpath
additionalarg2='--jars '+ jarpath

arglist =list((readuser,readpassword,writeuser,writepassword,readIP,writeIP,additionalarg1,additionalarg2,jarpath))




# STEP 3: Set default arguments for the DAG
default_dag_args = {
	'start_date': yesterday,
	'depends_on_past': False,
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 1,
	'retry_delay': timedelta(minutes=5),
	'project_id':models.Variable.get('project_id')
}

# STEP 4: Define DAG
# set the DAG name, add a DAG description, define the schedule interval and pass the default arguments defined before
#dag = DAG(
#	dag_id='mydag',
#	description='DAG for deployment a Dataproc Cluster',
#	schedule_interval=timedelta(days=1),
#	default_args=default_dag_args
#
#	)
with models.DAG(
	'demodag4',
	description='DAG for deployment a Dataproc Cluster',
	schedule_interval=timedelta(days=7),
	default_args=default_dag_args) as dag:

# STEP 5: Set Operators
	# BashOperator
	# A simple print date
	print_date = BashOperator(
		task_id='print_date',
		bash_command='date')

	

	# dataproc_operator
	# Create small dataproc cluster
	create_dataproc =  dataproc_operator.DataprocClusterCreateOperator(		
		task_id='create_dataproc',
		cluster_name='dataproc-cluster-demo-{{ ds_nodash }}',
		num_workers=2,
		zone=models.Variable.get('dataproc_zone'),
		master_machine_type='e2-standard-4',
		worker_machine_type='e2-standard-8')

	# Run the PySpark job
	
	
	run_spark0 = dataproc_operator.DataProcPySparkOperator(
		task_id='run_spark0',
		main=SPARK_CODE0,
		dataproc_pyspark_jars=jarpath,
		arguments = arglist,		
		cluster_name='dataproc-cluster-demo-{{ ds_nodash }}',
		job_name=dataproc_job_name+'0')
	
	
	run_spark1 = dataproc_operator.DataProcPySparkOperator(
		task_id='run_spark1',
		main=SPARK_CODE1,
		dataproc_pyspark_jars=jarpath,
		arguments = arglist,		
		cluster_name='dataproc-cluster-demo-{{ ds_nodash }}',
		job_name=dataproc_job_name+'1')
	
	
	run_spark2 = dataproc_operator.DataProcPySparkOperator(
		task_id='run_spark2',
		main=SPARK_CODE2,
		dataproc_pyspark_jars=jarpath,
		arguments = arglist,		
		cluster_name='dataproc-cluster-demo-{{ ds_nodash }}',
		job_name=dataproc_job_name+'2')


	run_spark3 = dataproc_operator.DataProcPySparkOperator(
		task_id='run_spark3',
		main=SPARK_CODE3,
		dataproc_pyspark_jars=jarpath,
		arguments = arglist,		
		cluster_name='dataproc-cluster-demo-{{ ds_nodash }}',
		job_name=dataproc_job_name+'3')

	# BashOperator
	# Sleep function to have 1 minute to check the dataproc created
	#sleep_process = BashOperator(
	#    task_id='sleep_process',
	#    bash_command='sleep 60'
	#    )
	# dataproc_operator


	# Delete Cloud Dataproc cluster.
	delete_dataproc = dataproc_operator.DataprocClusterDeleteOperator(
		task_id='delete_dataproc',
		cluster_name='dataproc-cluster-demo-{{ ds_nodash }}',
		trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

# STEP 6: Set DAGs dependencies
# Each task should run after have finished the task before.
	print_date >> create_dataproc >> run_spark0  >> run_spark1 >> run_spark2 >> run_spark3 >>  delete_dataproc







