import json
import boto3
import requests
import mysql.connector
from datetime import date

def connect_to_mysql(host, user, password, database):
  try:
    mydb = mysql.connector.connect(
      host=host,
      user=user,
      password=password,
      database=database
    )
    return mydb
  except mysql.connector.Error as err:
    print("Error connecting to MySQL:", err)
    return None
  
def execute_query(mydb, query):
  try:
    mycursor = mydb.cursor()
    mycursor.execute(query)
    myresult = mycursor.fetchall()
    return myresult
  except mysql.connector.Error as err:
    print("Error executing query:", err)
    return None

def lambda_handler(event, context):
  # Get S3 object details from the event
  bucket = event['Records'][0]['s3']['bucket']['name']
  key = event['Records'][0]['s3']['object']['key']

  # Download the JSON file from S3
  s3_client = boto3.client('s3')
  response = s3_client.get_object(Bucket=bucket, Key=key)
  data_json = response['Body'].read().decode('utf-8')
  customer_id_list = json.loads(data_json)

  # Connect to  database 

  mysql_host = "aws-simplified.c9kkw0gi2zxk.us-east-1.rds.amazonaws.com"
  mysql_user = "admin"
  mysql_password = "admin123"
  mysql_database = "superstore"

  
  # Connect to MySQL
  mydb = connect_to_mysql(mysql_host, mysql_user, mysql_password, mysql_database)
  if not mydb:
    exit(1)

  



  sql_query=f"""select customerID, CustomerName
          from customers
           where customerID in {customer_id_list} ;
          """
  
  # Execute query and fetch results
  customer_data = execute_query(mydb, sql_query)
  if not customer_data:
    exit(1)  

  # Send data to API endpoint (replace with your API credentials)
  url = "https://virtserver.swaggerhub.com/wcd_de_lab/top10/1.0.0/add"
  headers = {"Content-Type": "application/json"}
  response = requests.post(url, headers=headers, json=customer_data)

  # Check for successful POST
  if response.status_code == 201:
    print("Data sent successfully!")
  else:
    print(f"Error sending data: {response.status_code}")

  return {
    'statusCode': response.status_code,
    'body': json.dumps(customer_data)
  }