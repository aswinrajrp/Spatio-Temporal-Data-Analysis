# Spatio-Temporal-Data-Analysis Using Apache Spark and Hadoop
Performed Hot Zone and Hot Spot Analysis on NYC Taxi trip dataset; Defined spatial queries to compute hotness of different zones; Applied Getis-Ord statistics to spatio-temporal big data to identify statistically significant hot spots


****INTRODUCTION****
Geospatial is a type of data that represents the information about objects, or events that occur in any
location in the world. The events or objects can be static like buildings or roads, earthquakes, etc. It can
also be dynamic like moving traffic, the spread of disease, etc. Geo-Spatial data often consists of three
attributes, the location (geographic), the characteristics of the object or the event, and the temporal
information (lifespan of the event or the object in its current state) [1]. In this project, a peer-to-peer taxi
service based in New York is looking to process the geospatial data collected from their users and run
multiple spatial queries on the data. Spatial queries are different from the traditional SQL queries and
require high-performance frameworks to process them without any latency in real-time. Apache Spark
contains a SQL framework called SparkSQL which can run spatial queries using a distributed in-memory
cluster computing mechanism.



****Description of Phase-1 Implementation****

The four spatial queries which are range query, range join query, distance query and distance join
query are run by implementing the functions ‘ST_Contains’ and ‘ST_Within’. These two functions are
considered helper functions for the four queries. There are two parameters R and P which represent the
geographical boundary of a town and cab request locations done by customers, respectively.

**ST_Contains():** 
If a given point is enclosed in this rectangle, this function returns true or false otherwise.

**ST_Within():** 
If the given two points lie within a specific distance, the function returns true or false otherwise.

**Range Query:** 
Using the function ST_Contains(), we need to determine all the points within R by using
the query rectangle R and a set of points P.

**Range Join Query:** 
All the pairs within the rectangle R are determined by considering the given sets of
rectangles R and a set of points P.

**Distance query:** 
Using the function ST_Within(), all the points that lie within a distance D from P are
determined by considering the fixed point location P and distance D.

**Distance join query:** 
Using the function ST_Within(), all such pairs such that the point p1 is within a
distance D from point p2 by considering the two sets of points P1 and P2 and a distance D.



****Description of Phase-2 Implementation****

**Hot Zone Analysis**
The hot zone analysis uses a rectangle dataset and a point dataset. For each rectangle, the number of
points located within the rectangle will be obtained. The more points a rectangle contains, the hotter (and
more profitable) it will be.

**ST_Contains()**
If a given point is enclosed in this rectangle, this function returns true or false otherwise.

We use the ST_Contains() function on the given dataset to identify whether the pickup points are
within the sets of boundary rectangles or not. These are given as sets of two points within the xy-plane
that join to form a diagonal of the rectangular ‘hot zone’ region. We compare all the pickup-points for
each rectangle and output an excel document that shows the number of pickup points for each
rectangular hot zone. The rectangles with the highest number of pickup points will be the most profitable.
The code below will output the final result:

**Hot Cell Analysis**
The ‘hot cell analysis’ applies spatial statistics to spatio-temporal big data to identify statistically
significant hot spots using Apache Spark.

We have a set of pairs of points, representing the latitude and the longitude values which are the
diagonal points forming a rectangular area. We also have various pickup points within a duration of one
month for all of these pairs. We can visualise this data in terms of a 3D space time graph, which we can
use to take into account the many neighbouring cells. In the code find the sum of pickups, sum of squares of pickups and the mean of
pickups to plug into our Getis-Ord statistic score formula for determining the hotness of the cell. 

**getNeighbourCellsCount()**
This function gets the number of neighbouring cells for a given cell. They are determined by their
position inside the 3D space time graph that resembles a cube and the value for each position is given in
the code below:

**computeGstatisticZScore()**
This function computes the Getis-Ord statistic score, which is the metric used to determine the
hotness of a cell. We individually calculate the numerator and the denominator components of the
equation and then divide them to get the result that is returned.


****Lessons Learned & Future Application:****

Doing this project gave me a great insight on understanding Geo-Spatial data analytics and its
applications. This being my first semester, this project helped me familiarize myself with the Scala
programming language and using Apache Spark and Hadoop framework. After completing the Hot
zone and Hot cell analysis, relating it with the Cab pickup data helped me understand its significance.
By determining the Hottest Zones/ Cells, Cab service providers can manage cab fares based on time &
location, predict demand and optimize their running costs using their past rides data. This analysis can
hence provide a new insight to the cab service providers that will help them increase their returns.
This application can be extended to any spatio-temporal domain analytics such as train/ air travel
schedules, rain/ weather patterns, ecological analytics using birds, sea animals’ movement data, cell
tracking in Microscopy etc. I understood that in all these applications, the basic concepts come down
to computing the Hot zones/ cells and making decisions based on the resultant data. Working with
Hadoop given in the template also gave me a hands-on experience with how clusters work in
distributing the data and computing simultaneously. Though we didn’t actually do in multiple
machines, I was able to see how the output data for the code we wrote was being computed by
multiple clusters simultaneously and results being computed in partitions (the part0, part1… files in
the output). 

I believe that with a strong foundation laid by this project, I can extend this knowledge in
performing any Hot zone/ cell analysis just by computing the hottest zones and Getis-Ord statistic
scores using the given data. The values we obtain can also help us in identifying the cold zone/ cells
which can prove to be useful in other multiple applications. Being a student of MCS specializing in Big
Data Systems myself, this course has intrigued me further to work on Geo-Spatial analytics. I’m looking
forward to taking more test data from the internet from multiple fields as I mentioned above and
performing hotspot analysis to further enhance my learning in this subject. I thank the professors for
giving me the opportunity to work on this field. 
