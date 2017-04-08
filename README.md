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
![Figure 1](https://i.imgsafe.org/9283875946.jpeg)

Spark consists of two main components:

1. Spark Driver

The Spark Driver is the process that contains your main() method and defines what the Spark application should do. 
This includes creating RDDs, transforming RDDs, and applying actions. 
Under the hood, when the Spark Driver runs, it performs two key activities:

a. Converts your program into tasks: Your application will contain zero or more transformations and actions, so it's the Spark Driver's responsibility to convert those into executable tasks that can be distributed across the cluster to executors. Additionally, the Spark Driver optimizes your transformations into a pipeline to reduce the number of actual transformations needed and builds an execution plan. It is that execution plan that defines how tasks will be executed and the tasks themselves are bundled up and sent to executors.

b. Schedules tasks for executors: From the execution plan, the Spark Driver coordinates the scheduling of each task execution. As executors start, they register themselves with the Spark Driver, which gives the driver insight into all available executors. Because tasks execute against data, the Spark Driver will find the executors running on the machines with the correct data, send the tasks to execute, and receive the results.

2. Executors

Spark Executors are processes running on distributed machines that execute Spark tasks. Executors start when the application starts and typically run for the duration of the application. They provide two key roles:

a. Execute tasks sent to them by the driver and return the results.

b. Maintain memory storage for hosting and caching RDDs.

The cluster manager is the glue that wires together drivers and executors. Spark provides support for different cluster managers, including Hadoop YARN and Apache Mesos. The cluster manager is the component that deploys and launches executors when the driver starts. You configure the cluster manager in your Spark Context configuration.