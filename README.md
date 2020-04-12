# 1660-final-project

Di Wu (diw26)  


The implementation contains the GUI java application runs on docker  
The application submit the Hadoop job to GCP through Google API java library  
The inverted Indexing will be run on GCP Dataproc clusters  

the structure of the repo:
Client.java: the java application with GUI  
WordCount.java: the hadoop application for inverted index, it's based on the example of WordCount, I forgot to rename it.  

in the docker folder, there are several files:  
* Dockerfile
* client.jar _the jar file of the GUI application_
* credential.json _the credential file for google authentication_
