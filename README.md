# Lake Challenge

## Local

To begin this challenge I started by trying locally perform what I wanted to happen in a cloud solution. Using PySpark I
ingested the .csv files as a DataFrame before changing the column names to something less complex and more readable in
my opinion. Afterwards I changed the datatype of the numerical columns, but unfortunately I had to leave the "geometry"
columns as a string.

Finally, I saved the processed DataFrames as .parquet files partitioning them into folders with the Hive partitioning
scheme of city=XXXX. This scheme could further be enhanced by first partitioning the data by country and afterwards by
city, but it was not done here.

The full script can be seen in [local_test.py](local_test.py).

## AWS

With the local solution done, I moved onto AWS where I first started by creating a S3 storage bucket to store the raw
data. Due to some limitations in knowledge of AWS I first did some research on ways to create what I wanted as a
solution. I settled on using AWS Glue to perform a similar processing to the one I did locally.

I started by using the Glue Studio Visual builder as basis to define the tasks required:

* Get data from the S3 bucket
* Apply mapping
* Store data

After this visual definition I converted the job to a script to be able to more easily control what would happen. I
performed the same operations as I did locally (i.e. change variable names and data types when applicable) and stored
the data a new bucket.

To be able to implement this process automatically I turned to AWS Lambda functions to create a function that would
trigger when a new file was uploaded to a given bucket. This function would simply call the Glue job with an argument
containing the name of the file that was upload. This file name would be cleaned-up in the Glue job for a first time to
remove trailing whitespaces and fix the spaces being sent as '+'. After reading and mapping the file, the argument file
name would be used once again, this retrieving the city name from the file name and using that to store the .parquet
file inside the folder with the nomenclature city=XXXX.

I feel like there may be better and more correct ways to approach this problem but this was the one I settled with due
to my knowledge and functionality it provided. As for access control both buckets were set to private. The lambda
function could only read the data as it was given a AWSLambdaRole and AWSGlueServiceRole to execute Glue jobs, while the
Glue Job itself required a AmazonS3FullAccess to be able to write data in the S3 output bucket.

The code for the lambda job can be seen in [lambda.py](lambda.py) and the Glue job in both [glueJob.json](glueJob.json)
and [glue_job.py](glue_job.py).