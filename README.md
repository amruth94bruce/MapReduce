# MapReduce

Used Hadoop map-reduce to derive some statistics from Yelp Dataset.
Used reduce side join and job chaining techniques.
Used In Memory Join technique for few problems.

The dataset comprises of three csv files, namely user.csv, business.csv and review.csv

1. UniqueBiz.java
Lists unique categories of business located in “Palo Alto” 

2. TopRated.java
Finds the top ten rated businesses using the average ratings

3. TopTen.java
List the  business_id , full address and categories of the Top 10 businesses using the average ratings

4.StanfordBiz.java
Lists the 'user id' and 'rating' of users that reviewed businesses located in Stanford 
