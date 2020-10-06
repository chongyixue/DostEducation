# Password and sensitive Information
Set up password and sensitive information as environmental variable
1. add this line in ~/.profile
  export PASSWORD="yourpassword"
`source ~/.profile`
 to take effect


# Code purpose
1. **create3tables.py** -- create 3 tables in the warehouse for convenience in joining later
	a. **programmaxseqlookup** to know what the maximum number of lessons for a particular program. 

	b. **userpartner\_lookup** table with columns (user)id, channel\_id, name.

	c. **program\_experience**table with columns user\_id, program\_id and prog\_start. This provides a table that keep tracks of the total number of courses that are consumed. 

2. **create\_facttable.py**, **addtofact.py**
	- creates main fact table 
3. **aggregate.py**, **aggregate_cum.py**
	- produces main aggregated table for queries

4. **addHML.py** -- optimized for the query: How many users completed > 50\% content schedule to them at the end of month 3?

