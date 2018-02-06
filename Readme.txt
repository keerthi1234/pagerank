NAME : keerthi sagar bukkapuram
id : 800954091
 
 
 
 Compilation steps for execution of page rank program : 
 
 1. first we give the input file microwiki.txt
 
 Following commands are used to execute the program :
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* PRNK.java 
jar -cvf PRNK.jar 
hadoop fs -rm -r /user/<username>/output
hadoop jar PRNK.jar  </user/username/input> </user/username/output>
hadoop fs -cat /user/<username>/output/*
 
 Follow the below steps to execute in eclipse :
 1.Rightclick on the program 
 2. Run as configuration
 3. give the arguments i.e input and outputpaths
 4.Apply changes
 5.Run.