# Password and sensitive Information
Set up password and sensitive information as environmental variable
1. add this line in ~/.profile
  export PASSWORD="yourpassword"
`source ~/.profile`
 to take effect


# Code purpose
1. **createtable\_programmaxseqlookup.py**: create a lookup table **programmaxseqlookup** to know what the maximum number of lessons for a particular program. 

2. **create\_userpartnerlookup.py** creates a **userpartner\_lookup** table with columns (user)id, channel\_id, name.

3. **create\_programexperience.py** creates a **program\_experience**table with columns user\_id, program\_id and prog\_start. This provides a table that keep tracks of the total number of courses that are consumed. 

