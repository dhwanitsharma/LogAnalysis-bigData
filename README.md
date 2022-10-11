#  Dhwanit Sharma - HW1 CS441
## University of Illinois at Chicago

## Introduction
This project is based on Apache Hadoop and contain 4 map-reduce program which are used in big data log analysis.
A map-reduce job includes a map phase and reduce phase. 

In this program, the mapper splits the log file line using split function. Then it splits the log message into key,value pair which works as input for the Reducer.
The value can be assigned as per the requirement of the Task.

The Reducer Of Map-Reduce  is consist of mainly 3 processes/phases:
1. **Shuffle:** Shuffling helps to carry data from the Mapper to the required Reducer.
2. **Sort:** In this phase, the output of the mapper that is actually the key-value pairs will be sorted on the basis of its key value.
3. **Reduce:** Once shuffling and sorting will be done the Reducer combines the obtained result and perform the computation operation as per the requirement.

## Project Structure
The project structure is as follows:
1. Src 
   1. main
      1. resources --- Contains the usefull resources and config file
         1. application.conf
         2. logback.xml
      2. scala --- Contains all the task files and helper files.
         1. Helper
            1. CreateLogger.scala
            2. Definitions.scala
         2. Task1.scala
         3. Task2.scala
         4. Task3.scala
         5. Task4.scala
   2. test --- Contains all the test files.
      1. scala
         1. TestApplicationConf.scala
         2. TestPattern.scala

## Installation Instructions
This sections contains all the instructions to install and run all the Map-reduce programs
1. Use the following URL to clone the project : git@github.com:dhwanitsharma/LogAnalysis-bigData.git
2. In the root directory, run the command "sbt assembly" and this will create a jar in the following path:target/scala-3.1.3 
3. Select a time interval in the application.conf. By default the time interval is 60 seconds. The time inteval is seconds example- for 2 mins, it should be set as 120
4. **Task1 :** takes 2 inputs - Input path, Output path. To run this Task the command will be as : hadoop jar {JarName} {InputPath} {OutputPath}. Example "hadoop jar LogAnalysis_bigData-assembly-0.1.0-SNAPSHOT.jar path/logfiles/input path/logfiles/Task1_Output"
5. **Task2 :**, takes 3 inputs - Input path,Temporary Output path, Output path. To run this Task the command will be as : hadoop jar {JarName} {InputPath} {TemporaryPath} {OutputPath}. Example "hadoop jar LogAnalysis_bigData-assembly-0.1.0-SNAPSHOT.jar path/logfiles/input path/logfiles/Task2Temp_Output path/logfiles/Task2_Output". The temporary output is explained in details in the Task 2 section.
6. **Task3 :**, takes 2 inputs - Input path, Output path. To run this Task the command will be as : hadoop jar {JarName} {InputPath} {OutputPath}. Example "hadoop jar LogAnalysis_bigData-assembly-0.1.0-SNAPSHOT.jar path/logfiles/input path/logfiles/Task3_Output"
7. **Task4 :**, takes 2 inputs - Input path, Output path. To run this Task the command will be as : hadoop jar {JarName} {InputPath} {OutputPath}. Example "hadoop jar LogAnalysis_bigData-assembly-0.1.0-SNAPSHOT.jar path/logfiles/input path/logfiles/Task4_Output"

## Log File Description
The input file for the programs will be a log file with a specific format where each line will have a log file such as "17:47:37.791 [scala-execution-context-global-25] WARN  HelperUtils.Parameters$ - Swq;g+6M:?820=Gmd#.p)sFaqo". 
Where we have TimeStamp:17:47:37.791, Error Message Type:WARN, Error Message:Swq;g+6M:?820=Gmd#.p)sFaqo in each line.

## Tasks Description
### Task1
Log files are used as input. The mapper will take a fixed interval which will be used to distribute the messages. This interval is set in application.conf. The error messages will be matched with the predefined pattern which is also set in the application.conf file.
The messages which will match the patterns will only be distributed in the pre-defined time intervals.

The output of the mapper will be the timestamp interval, message type  (INFO, DEBUG, ERROR, WARN), and interval count as 1. The reducer phase aggregates this and outputs number of messages of a particular message type in a given interval.

Example output:17:46:00 WARN,5

Here 17:46:00 is the Timestamp, WARN is the error message type, 5 is the total number in the given TimeInterval
### Task2
### Task3
### Task4

## AWS EMR Deployment
As shown in the video, build the file using "sbt compile" to build the jar. Upload the logfiles in a folder in a S3 bucket.
Give the input path and the output path as shown in the video.
