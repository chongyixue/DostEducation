# DostEducation
Insight Project

# Problem Statement
 Dost Education is a non-profit Education Tech company. Parents sign up either directly, or through partner origanisations, which then receives daily 1 or 2 minute phone calls for activity suggestions to do with their child to promote early developmental learning. Their transactional database is not optimal for analytical purpose and reporting. 

# Solution
A data warehouse will be built for the purpose of analytics and visualization through Chartio. Batch processing will be done on the google-managed PostgreSQL database.

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

3. **programseq**\\
Key columns:
	- program\_id
	- sequence\_index
	- week
	- day

There are currently 28 distinct program\_id. Programs typically take 6 months, or 112 sequences.

4. **experience** \\
Whenever a user signs for a new program, or changed their phone number to be reached, a new experience entry is recorded.
Key columns:
	- user\_id
	- provider\_number
	- phone
Provider\_number is the number that a user calls to sign up for a particular program. This number uniquely defines through which partner a user signed up for a program in Dost.





# Data Warehouse Schema




