## Pyspark Playground
Experimenting with PySpark and working along with [Spark and Python for Big Data with PySpark](https://www.udemy.com/course/spark-and-python-for-big-data-with-pyspark) by Pierian Data on Udemy. 

## Setup on EC2 ubuntu

```
sudo apt-get update
sudo apt install python3-pip
pip3 install -r requirements.txt
sudo apt-get install jre   # java
sudo apt-get install scala
wget https://dlcdn.apache.org/spark/spark-3.2.0/spark-3.2.0-bin-hadoop3.2.tgz
sudo tar -zxvf spark-3.2.0-bin-hadoop3.2.tgz
```

## Setting up ssh for displaying jupyter locally
https://jupyter-notebook.readthedocs.io/en/stable/public_server.html


## Using finspark to find the spark install
```
pip3 install findspark
```

In script or notebook

```
import findspark
findspark.init('/home/ubuntu/spark-3.2.0-bin-hadoop3.2') # wherever your spark is 
```



