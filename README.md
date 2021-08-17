# Realtime device monitoring with Apache Flink on Kinesis Data Analytics


## Reference architecture

![kda1](/images/kda1.PNG)

## Create a Kinesis Data Analytics Studio notebook
1. Go to Kinesis Data Analytics Console: console.aws.amazon.com/kinesisanalytics
2. Click on the Studio tab
3. Click on create Studio notebook
4. Choose "Quick create with sample code" as create method
5. Enter a notebook name
6. For AWS Gluedatabse click on the refresh button and select Default Glue database. If the list is still empty, create a new Glue database.
7. Note down the IAM role name. Click on Create Studio Notebook.

![kda1](/images/kda2.png)


## Configure IAM
1. Go to IAM: https://console.aws.amazon.com/iam
2. Click on the role and search for the role that KDA Studio has created earlier
3. Click on Attach Policies and add Administrator Access. (This is not recommended for your production workload.)

![kda1](/images/kda3.png)

## Working with Kinesis Data Analytics Studio
1. Go to Kinesis Data Analytics Console: console.aws.amazon.com/kinesisanalytics
2. Click on the Studio tab and select the notebook you have created in the previous step
3. Click on Run and Click on Open in Apache Zeppelin once the statue of the Notebook is running
![kda4](/images/kda4.PNG)

## Working with Kinesis Data Analytics Studio - create a prerequisite notebook
1. In the notebook console create a new note
2. enter the name of your notebook- "prerequisite"
3. Select Default Interprete as Flink and create the notebook
![kda5](/images/kda5.png)

** We are going to use the notebook to provisioned some AWS resources, for example, DynamoDB table, Kinesis Data Streams etc. For that, we are using boto3. 
4. execute the following code to install boto3
```
%flink.ipyflink

pip install boto3
```

5. Create a new paragraph and execute the below code. This will create a DynamoDB table in ap-southeast-2
```
%flink.ipyflink
#create table lab3
import boto3
region='ap-southeast-2'
dynamodb = boto3.resource('dynamodb',region_name=region)
response = dynamodb.create_table(
    AttributeDefinitions=[
        {
            'AttributeName': 'pk',
            'AttributeType': 'S'
        },
    ],
    TableName='lab3',
    KeySchema=[
        {
            'AttributeName': 'pk',
            'KeyType': 'HASH'
        },
    ],
    BillingMode='PAY_PER_REQUEST'
)
```
![kda6](/images/kda6.png)


6. Create a new paragraph and execute the below code. This will create a Kinesis data stream in ap-southeast-2
```
%flink.ipyflink
#create KDS lab3
import boto3
region='ap-southeast-2'
kinesis = boto3.client('kinesis',region_name=region)
response = kinesis.create_stream(
    StreamName='lab3',
    ShardCount=3
)
print (response)

```

## Uploading sample data
1. Create a new S3 bucket or upload the below CSV files to your S3 bucket

    a) [latlon.csv](sampledata/latlon.csv)
 
 2. Create a new paragraph on your prerequisite notebook and execute the below code. Change the S3 location as your's (bucket, key). This will upload the latlon data to a DynamoDB table you created earlier.
 
 ```
 %flink.ipyflink
#upload lanlon data
import boto3
import csv
import codecs
region='ap-southeast-2'
recList=[]
tableName='lab3'
s3 = boto3.resource('s3')
dynamodb = boto3.client('dynamodb', region_name=region)
bucket='YOUR_BUCKETNAME'
key='lab3/latlon.csv'
obj = s3.Object(bucket, key).get()['Body']
batch_size = 100
batch = []
i=0

for row in csv.DictReader(codecs.getreader('utf-8')(obj)):
    pk= (row["id"])
    postcode= (row["postcode"])
    suburb= (row["suburb"])
    State= (row["State"])
    latitude= (row["latitude"])
    longitude= (row["longitude"])
    
    response = dynamodb.put_item(
        TableName=tableName,
        Item={
        'pk' : {'S':str(pk)},
        'postcode': {'S':postcode},
        'suburb': {'S':suburb},
        'State': {'S':State},
        'latitude': {'S':latitude},
        'longitude': {'S':longitude}
        }
    )
    i=i+1
    #print ('Total insert: '+ str(i))
    
print ('completed')
 ```


## Generating random data to Kinesis data streams
1. Create a new paragraph on your prerequisite notebook and execute the below code. This will generate random modem data and send those data to a Kinesis Data Streams.

```
%flink.ipyflink
#Random DG -2
import json
import boto3
import csv
import datetime
import random
from boto3.dynamodb.conditions import Key
tablename='lab3'
kdsname='lab3'
region='ap-southeast-2'
i=0
clientkinesis = boto3.client('kinesis',region_name=region)

#Schema: Offername, lat, lon,state,postcode, suburb,cdate

def getproduct(i):
    product=["Massive500GB-$65mth", "NowOnly-$1mth", "NowOnly-$5mth", "Save$499-SamsungGalaxy", "iPad ProSupercharged by Apple M1Chip"]
    return (product[i])
    
def getlanlon():
    dynamodb = boto3.resource('dynamodb',region_name=region)
    table = dynamodb.Table(tablename)
    randomnum = random.randint(5498, 10994)
    response = table.query(
        KeyConditionExpression=Key('pk').eq(randomnum)
    )
    items=response['Items']
    #lat='222'
    #lon='123'
    for item in items:
        lat=item['latitude']
        lon=item['longitude']
        state=item['State']
        postcode=item['postcode']
        suburb=item['suburb']
    return lat, lon, state, postcode, suburb

#Schema: Offername, lat, lon,state,postcode, suburb,event_time
#Schema: model: NetComm, Deviceid: 1800, interface: eth4.1, interfacestatus: connected, CPU: 90, Memory: 1203, lat, lon,state,postcode, suburb,event_time


def getModel():
    product=["Ultra WiFi Modem", "Ultra WiFi Booster", "Netgear EVG2000", "Sagemcom Fast 5366 TN", "ASUS AX5400"]
    randomnum = random.randint(0, 4)
    return (product[randomnum])

def getInterfaceStatus():
    status=["connected", "connected", "connected", "connected", "connected", "connected", "connected", "connected", "connected", "connected", "connected", "connected", "down", "down"]
    randomnum = random.randint(0, 13)
    return (status[randomnum])

def randomoffer(i):
    product=["Massive500GB-$65mth", "NowOnly-$1mth", "NowOnly-$5mth", "Save$499-SamsungGalaxy", "iPad ProSupercharged by Apple M1Chip"]
    return (product[i])

def getCPU():
    i = random.randint(50, 100)
    return (str(i))

def getMemory():
    i = random.randint(1000, 1500)
    return (str(i))


while True:
    
    i=int(i)+1
    model=getModel()
    deviceid='dvc' + str(random.randint(3001, 6000))
    interface='eth4.1'
    interfacestatus=getInterfaceStatus()
    cpuusage=getCPU()
    memoryusage=getMemory()
    event_time=datetime.datetime.now().isoformat()
    lat, lon,state,postcode, suburb=getlanlon()
    location=str(lat) + ", " + str(lon)
    
    new_dict={}
    new_dict["model"]=model
    new_dict["deviceid"]=deviceid
    new_dict["interface"]=interface
    new_dict["interfacestatus"]=interfacestatus
    new_dict["cpuusage"]=cpuusage
    new_dict["memoryusage"]=memoryusage
    new_dict["event_time"]=event_time
    #new_dict["lat"]=lat
    #new_dict["lon"]=lon
    new_dict["location"]=location
    new_dict["state"]=state
    new_dict["postcode"]=postcode
    new_dict["suburb"]=suburb
    #print(json.dumps(new_dict))
    #clientkinesis.put_record(kdsname, json.dumps(new_dict), prodcat)
    clientkinesis.put_record(
                    StreamName=kdsname,
                    Data=json.dumps(new_dict),
                    PartitionKey=deviceid)
    #print(str(lat) + ","+ str(lon))
    
print('###total rows:#### '+ str(i))
```

## Real-time analytics with Flink SQL
1. In the notebook console create a new note
2. enter the name of your notebook- "flinkSQLExample"
3. Select Default Interprete as Flink and create the notebook
4. Execute the below code

```
%flink.ssql

CREATE TABLE devicestatus (
    model VARCHAR(50),
    deviceid VARCHAR(50),
    interface VARCHAR(50),
    interfacestatus VARCHAR(50),
    cpuusage DOUBLE,
    memoryusage DOUBLE,
    --lat VARCHAR(20),
    --lon VARCHAR(20),
    location VARCHAR(100),
    state VARCHAR(20),
    postcode VARCHAR(30),
    suburb VARCHAR(30),
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
)
PARTITIONED BY (deviceid)
WITH (
    'connector' = 'kinesis',
    'stream' = 'lab3',
    'aws.region' = 'ap-southeast-2',
    'scan.stream.initpos' = 'LATEST',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
)

```
5. Add a new paragraph and start analyzing data in real-time
```
%flink.ssql(type=update)

SELECT * FROM devicestatus;

```
![kda7](/images/kda7.png)

6.  Add a new paragraph and execute the below code
```
%flink.ssql(type=update)
--statewise up time
SELECT devicestatus.interfacestatus, COUNT(*) AS totalstatus, devicestatus.state,
       TUMBLE_END(event_time, INTERVAL '10' second) as tum_time
  FROM devicestatus
GROUP BY TUMBLE(event_time, INTERVAL '10' second), devicestatus.interfacestatus,devicestatus.state;

```
![kda8](/images/kda8.png)

7. Add a new paragraph and execute the below code
```
%flink.ssql(type=update)
--device model wise up time
SELECT devicestatus.interfacestatus, COUNT(*) AS totalstatus, devicestatus.model,
       TUMBLE_END(event_time, INTERVAL '10' second) as tum_time
  FROM devicestatus
GROUP BY TUMBLE(event_time, INTERVAL '10' second), devicestatus.interfacestatus,devicestatus.model;

```
![kda9](/images/kda9.png)

8. Add a new paragraph and execute the below code
```
%flink.ssql(type=update)
-- The task of the following example is to find the longest period of time for which the average CPUUsage of a device did not go below certain threshold.
SELECT deviceid, avgCPUUsage
FROM devicestatus
    MATCH_RECOGNIZE (
        PARTITION BY deviceid
        ORDER BY event_time
        MEASURES
            FIRST(A.event_time) AS start_tstamp,
            LAST(A.event_time) AS end_tstamp,
            AVG(A.cpuusage) AS avgCPUUsage
        ONE ROW PER MATCH
        AFTER MATCH SKIP PAST LAST ROW
        PATTERN (A+ B) WITHIN INTERVAL '1' HOUR
        DEFINE
            A AS AVG(A.cpuusage) > 95
    )

```
![kda10](/images/kda10.png)

