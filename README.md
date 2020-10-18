# DostEducation
Insight Project

# Problem Statement
 Dost Education is a non-profit Education Tech company. Parents sign up either directly, or through partner origanisations, which then receives daily 1 or 2 minute phone calls for activity suggestions to do with their child to promote early developmental learning. Their transactional database is not optimal for analytical purpose and reporting. 

# Solution
A data warehouse will be built for the purpose of analytics and visualization through Chartio. Batch processing will be done on the google-managed PostgreSQL database. Detailed information on setting up Google Cloud Platform for data transformation through a dataproc cluster into the data warehouse, plus google composer setup for Airflow weekly update is in the README filein the **composer** section. 

# Database Schema (Simplified)
![Postgresql Tables](/images/DBsimplified.png)

1. **users**<br/>
This table records every user who signed up. Key columns:
	- created\_on

2. **campaign**<br/>
Records every phone calls to each user. Key columns:
	- experience\_id 
	- program\_id
	- user\_id
	- listen\_secs
	- timestamps:
		- attempted\_timestamp
		- call\_start\_time
		- call\_end\_time
	- programseq\_id

3. **programseq**<br/>
Key columns:
	- program\_id
	- sequence\_index
	- week
	- day

There are currently 28 distinct program\_id. Programs typically take 6 months, or 112 sequences.


4. **experience**<br/>
Whenever a user signs for a new program, or changed their phone number to be reached, a new experience entry is recorded.
Key columns:
	- user\_id
	- provider\_number
	- phone
Provider\_number is the number that a user calls to sign up for a particular program. This number uniquely defines through which partner a user signed up for a program in Dost.

5. **content**<br/>
This table has metadata on length of each content, i.e. podcast episodes. 

6. **listen\_rate**<br/> (PostgreSQL view)
This view combines the campaign table where status are either completed missed, and the content table to have tabulated information on content length sent out on each call.
 


# Data Warehouse Schema-The Plan
All of the tables created in this section will be stored in a google-managed PostgreSQL database. The warehouse contains smaller lookup tables joined from the database for easier joins to form the main fact table. 

## Fact Table
The fact table schema can be modified, e.g. adding columns if metric requirements change. 
1. **mainfact**
	- Keys
		- User_id
		- Program_id
		- content\_id
		- channel\_id
	- Facts
		- programstartmonth\_abs
		- Deploymonth\_abs
		- ab_months\_in
		- prog\_start
		- listen
		- duration\_sec
		- maxseq	

## Skeleton of the Data Warehouse
From the **mainfact** table, aggregations for different metric can be done such that the information is stored in a small table with limited rows to be directly queried by Chartio. For this MVP we have look particularly into metrics that require aggregation of total seconds listened by month, e.g. percentage of users registered in 2020 that has listened to more than 50\% of content in terms of seconds, when they are 3 months into the program.

Then, we have aggregation tables **aggmonth** and **HMLmonth** that aggregate users by month into High-Medium-Low categories. Finally, **year2020HML** table is the most specific table that Chartio can query directly.













