# SparkExample

This is a simple example program to use Apache Spark that I followed this [tutorial](http://www.javaworld.com/article/2972863/big-data/open-source-java-projects-apache-spark.html).

Steps to run:
1. mvn clean install
2. java -jar target/spark-example-1.0-SNAPSHOT.jar input.txt

Summary: Data analysis with Spark

The steps for analyzing data with Spark can be grossly summarized as follows:
1. Obtain a reference to an RDD.
2. Perform transformations to convert the RDD to the form you want to analyze.
3. Execute actions to derive your result set.

An important note is that while you may specify transformations, they do not actually get executed until you specify an action. 
This allows Spark to optimize transformations and reduce the amount of redundant work that it needs to do. 
Another important thing to note is that once an action is executed, you'll need to apply the transformations again in order to execute more actions. 
If you know that you're going to execute multiple actions then you can persist the RDD before executing the first action by invoking the persist() method; just be sure to release it by invoking unpersist() when you're done.


Spark in a distributed environment:
