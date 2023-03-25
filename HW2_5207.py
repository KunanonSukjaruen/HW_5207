import boto3
import botocore
import pandas as pd
from IPython.display import display, Markdown

s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')

def create_bucket(bucket):
    import logging

    try:
        s3.create_bucket(Bucket = bucket)
    except botocore.exceptions.ClientError as e:
        logging.error(e)
        return 'Bucket ' + bucket + ' could not be created.'
    return 'Created or already exists ' + bucket + ' bucket.'

create_bucket('nyctlc-cs653-5207')

def key_exists(bucket, key):
    try:
        s3_resource.Object(bucket, key).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            # The key does not exist.
            return(False)
        else:
            # Something else has gone wrong.
            raise
    else:
        # The key does exist.
        return(True)

def copy_among_buckets(from_bucket, from_key, to_bucket, to_key):
    if not key_exists(to_bucket, to_key):
        s3_resource.meta.client.copy({'Bucket': from_bucket, 'Key': from_key}, 
                                        to_bucket, to_key)        
        print('File {} saved to S3 bucket {}'.format(to_key, to_bucket))
    else:
	print('File {} already exists in S3 bucket {}'.format(to_key, to_bucket)) 

copy_among_buckets(from_bucket='nyc-tlc', from_key = 'trip data/yellow_tripdata_2017-01.parquet',
                      to_bucket='nyctlc-cs653-5207', to_key='trip-data/yellow_2017-01.parquet')
copy_among_buckets(from_bucket='nyc-tlc', from_key = 'trip data/yellow_tripdata_2017-02.parquet',
                      to_bucket='nyctlc-cs653-5207', to_key='trip-data/yellow_2017-02.parquet')
copy_among_buckets(from_bucket='nyc-tlc', from_key = 'trip data/yellow_tripdata_2017-03.parquet',
                      to_bucket='nyctlc-cs653-5207', to_key='trip-data/yellow_2017-03.parquet')


#//////////////////////// ข้อ A //////////////////////////////////////////
import boto3

s3 = boto3.client('s3')

resp = s3.select_object_content(
    Bucket='nyctlc-cs653-5207',
    Key='trip-data/yellow_2017-01.parquet',
    ExpressionType='SQL',
    Expression="SELECT payment_type FROM s3object",
    InputSerialization={'Parquet': {}},
    OutputSerialization={'CSV': {}},
)

# Process the response to extract the result of the SELECT operation
payment_type_counts = {}
for event in resp['Payload']:
    if 'Records' in event:
        records = event['Records']['Payload'].decode('utf-8').strip().split('\n')
        for record in records:
            payment_type = int(record)
            payment_type_counts[payment_type] = payment_type_counts.get(payment_type, 0) + 1

# Show the result
print("Number of rides by payment type:", payment_type_counts)

#//////////////////////// ข้อ B ////////////////////////////////////////
import boto3

s3 = boto3.client('s3')
bucket = 'nyctlc-cs653-5207'
key = 'trip-data/yellow_2017-01.parquet'

def get_query_result(expression):
    select_results = s3.select_object_content(
        Bucket=bucket,
        Key=key,
        Expression=expression,
        ExpressionType='SQL',
        InputSerialization={'Parquet': {}},
        OutputSerialization={'CSV': {}},
    )

    result = 0.0
    for event in select_results['Payload']:
        if 'Records' in event:
            result = float(event['Records']['Payload'].decode('utf-8'))

    return result

for i in range(1, 266):
    count_query = "SELECT count(PULocationID) FROM s3object WHERE PULocationID = {}".format(i)
    fare_query = "SELECT sum(fare_amount) FROM s3object WHERE PULocationID = {}".format(i)
    avg_passenger_query = "SELECT avg(passenger_count) FROM s3object WHERE PULocationID = {}".format(i)

    count = int(get_query_result(count_query))
    fare_sum = get_query_result(fare_query)
    avg_passenger = get_query_result(avg_passenger_query)

    print("No. of rides in Location {} = {}".format(i, count))
    print("Sum fare amount in Location {} = {:.2f}".format(i, fare_sum))
    print("Average no. of passenger in Location {} = {:.2f}".format(i, avg_passenger))

    #//////////////////////// ข้อ C ////////////////////////////////////////

import boto3

s3 = boto3.client('s3')
bucket = 'nyctlc-cs653-5207'

months = ['01', '02', '03']
month_names = ['JAN', 'FEB', 'MAR']

def get_query_result(expression, month):
    select_results = s3.select_object_content(
        Bucket=bucket,
        Key='trip-data/yellow_2017-{}.parquet'.format(month),
        Expression=expression,
        ExpressionType='SQL',
        InputSerialization={'Parquet': {}},
        OutputSerialization={'CSV': {}},
    )

    result = 0.0
    for event in select_results['Payload']:
        if 'Records' in event:
            result = float(event['Records']['Payload'].decode('utf-8'))

    return result

for i, month in enumerate(months):
    print("2017 Month: {}".format(month_names[i]))
    for type in range(1,6):
        query = "SELECT count(payment_type) FROM s3object s WHERE payment_type = {}".format(type)
        count = int(get_query_result(query, month))
        print("payment_type {} = {}".format(type, count))
    print("**********************")
