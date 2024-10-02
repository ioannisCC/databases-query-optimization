
1.	**Query 1**: The goal was to find the number of location points (lon, lat) per calendar day and sort the result in descending order by the number of points. This was done using a SELECT statement for the required fields and a GROUP BY clause to count the location points.
2.	**Query 2**: The task was to find how many Greek-flagged ships exist per ship type. This was achieved by selecting the type field and grouping the results by the ship type.
3.	**Query 3**: There were two parts:
	•	First, to identify ships that at some point exceeded a speed of 30 knots.
	•	Second, to find the type of these ships and how many of each type existed. The DISTINCT clause was used to ensure unique ships were considered, even if a ship exceeded 30 knots multiple times.
4.	**Query 4**: For passenger ships (those with types starting with “passenger”), the number of location points recorded per day during the period 14/08/2019 - 18/08/2019 was calculated. The timestamp field was used within the date range.
5.	**Query 5**: For cargo ships:
	•	The goal was to find which ships were anchored (speed zero) during the period 15/08/2019 - 18/08/2019, using a date range and condition on speed in the WHERE clause.
	•	The second part focused on the entire period 12/08/2019 - 19/08/2019, calculating the total sum of ship speeds in that time. If the sum was less than 1, the ship was considered anchored throughout the period.

###Optimization###:

	Buffers and Workers:
	PostgreSQL initially used 128 MB buffers, which were increased to 8 GB, leading to improved response times.
	The number of workers was increased from 8 to 1024 to further optimize performance.
 
**Indexing**:
	Sequential scans were disabled using SET enable_seqscan=off to force PostgreSQL to use indexes instead of sequential table reads.
	Five indexes were created:
	B+Tree indexes on speed, lon, lat, and t (all fields of the positions table), since the queries on these fields involved ranges.
	A hash index on the flag field of the Vessels table, as queries on this field used equality comparisons.
	A B+Tree index on the description field of the VesselTypes table, to optimize LIKE queries.
The indexing resulted in varying performance improvements. Some queries showed significant improvements, while others showed little or no benefit, with one query even experiencing a performance decline.

**Partitioning**:
	Declarative partitioning was used, splitting the positions table (the largest table) into three partitions based on 10-day intervals. This approach did not yield significant improvements and was later changed to partition by individual days.
	The revised partitioning method showed slight performance gains, but indexing did not work optimally with partitioning, leading to inconsistent improvements in query times.

In conclusion, while indexing and partitioning improved performance in some cases, the combination of partitioning and indexing did not always provide the desired results, and further adjustments were needed for better optimization.

