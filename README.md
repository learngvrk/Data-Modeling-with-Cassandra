# Data-Modeling-with-Cassandra
Data-Modeling-with-Apache Cassandra (Udacity: Data Engineering Nano Degree) | learngvrk@gmail.com | 2019-12-27 This project is a part of Udacity's Data Engineer Nano Degree.

## Project 2 - Data Modeling with Apache Cassandra

![MUSIC DATA ANALYTICS](Music_App_Analytics.jpg)

<b>Courtesy:</b> Adobe Stock Images

### Background:
The startup Sparkify was interested to analyze the data collected from the user activity on their music app on the mobile phones.

Based on the user activity the startup would like to perform some analytics to derive insights which will help the organization to better understand the user behavior, and so add more interesting features to their mobile app which would enhance the user experience and stratergize the product development roadmap.

### Business Need:

The business is looking to answer the following questions or would like to view the analyze the data obtained from the obtained answer (query results).

1. Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4
2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

### Database Schema

- For Apache Cassandra, the best strategy would be to have <b>one TABLE per one QUERY.</b>
- Artists, Users, Songs are dimensions needed to build the tables, which is then used to derive insights.
- Since these tables are used for analytics they are expected to be <b>DE-NORMALIZED.</b>

![TABLE1 SCHEMAS](images/Query_table1.png)

![TABLE2 SCHEMAS](images/Query_table2.png)

![TABLE3 SCHEMAS](images/Query_table3.png)

### Perform Normalization
> 1. Ensured the Dimension tables meet the 3NF (Normalization Form)
> 2. Most Important features of the selected Dimension data are used as table columns.
> 3. One-to-Many relationship with the Fact (OLAP) Table.

### Create Dimension Tables
Created the following **DIMENSION** tables
> 1. Users: user info (columns: user_id, first_name, last_name, gender, level)
> 2. Songs: song info (columns: song_id, title, artist_id, year, duration)
> 3. Artists: artist info (columns: artist_id, name, location, latitude, longitude)
> 4. Time: detailed time info about song plays (columns: start_time, hour, day, week, month, year, weekday)

### Perform Denormalization
> 1. Ensured the Fact tables have all the primary keys of the Dimension tables.
> 2. All the categories of the Dimension tables are included within the Fact tables.
> 3. All the required measures can be calculated using the aggregation function performed on the categories (data).

### Create the FACT table: 
> songplays: song play data together with user, artist, and song info (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

### SQL Queries
<ol>
  <li> Establish connection to the local instance of the Postgres DB.</li>
  <li> Create CREATE TABLE statements to Create Dimension and Fact tables.</li>
  <li> Create INSERT INTO TABLE statements to enter the songs and log data into the Dimension and Fact table.</li>
</ol>

### Perform ETL
<ol>
<li> Extract data from songs data JSON file to fill in the **SONGS** and **ARTISTS** dimension table.</li>
<li> Extract data from log data JSON file to fill in the **USERS** and **TIME** dimension table.</li>
<ol>
  <li> Timestamp is represented as milliseconds in the log data.</li>
  <li> Timestamp is transformed into **Time (hh:mm:ss), Hour, Day, Week of Year, Month, Year, Day of Week**.</li>
</ol>
<li> Extract the log data, and data from dimension tables to fill in the **SONGPLAYS** FACT table.</li>
</ol>

### Convert Jupter Note Book Code into Modular Python code (.py) file
1. Convert the Python Scripts from the web kernel to modular Python code (.py) file.
2. Create common functions to perform the database and ETL functions.


### Sample Queries which can be used for Analytics
- For a given user what is his favorite songs (most played ones)
- What is the most played songs (Toppers) at a given time of the day or season based on user demographics
- Is there a song which is played across all regions of the country.
