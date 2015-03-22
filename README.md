# MapReduce
A custom mapreduce framework that is based off Hadoop. It has one master and an arbitrary 
number of worker nodes. The framework dynamically loads jobs and allows the user to input the 
number of threads per mapper and reducer. It runs on Jetty and uses REST to communicate between 
the master and worker nodes. 


