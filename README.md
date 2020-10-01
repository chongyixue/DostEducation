# DostEducation
Insight Project

# Problem Statement
 Dost Education is a non-profit Education Tech company. Parents sign up either directly, or through partner origanisations, which then receives daily 1 or 2 minute phone calls for activity suggestions to do with their child to promote early developmental learning. Their transactional database is not optimal for analytical purpose and reporting. 

# Solution
A data warehouse will be built for the purpose of analytics and visualization through Chartio. Batch processing will be done on the google-managed PostgreSQL database.

# Database Schema (Simplified)
![Postgresql Tables](/images/DBsimplified.png)

1. Campaign: records every phone calls to each user. Key columns:
	- experience\_id 
	- program\_id
	- user\_id
	- listen\_secs
	- timestamps:
		- attempted\_timestamp
		- call\_start\_time
		- call\_end\_time
	- programseq\_id



# Data Warehouse Schema




