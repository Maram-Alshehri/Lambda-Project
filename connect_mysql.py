import mysql.connector
import boto3
import json

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

def save_to_json(data, filename):
  try:
    # Convert Decimal to float before dumping to JSON
    data = [[row[0], float(row[1])] for row in data]  # Loop through rows and convert
    with open(filename, 'w') as outfile:
      json.dump(data, outfile)
    print(f"Data saved to JSON: {filename}")
  except Exception as err:
    print("Error saving data to JSON:", err)

def upload_to_s3(filename, bucket_name, s3_folder):
  try:
    s3 = boto3.client('s3')
    s3.upload_file(filename, bucket_name, f"{s3_folder}/{filename}")
    print(f"File uploaded to S3: s3://{bucket_name}/{s3_folder}/{filename}")
  except Exception as err:
    print("Error uploading file to S3:", err)

if __name__ == "__main__":
  # Replace with your RDS database details
  mysql_host = "aws-simplified.c9kkw0gi2zxk.us-east-1.rds.amazonaws.com"
  mysql_user = "admin"
  mysql_password = "admin123"
  mysql_database = "superstore"
  sql_query = "SELECT customer_id, SUM(sales) AS total_sales FROM orders GROUP BY customer_id ORDER BY total_sales DESC LIMIT 10;"

  # Replace with your S3 bucket and folder names
  s3_bucket_name = "wcd-lambda-project-maram"
  s3_folder = "input"
  filename = "top_customer_sales.json"

  # Connect to MySQL
  mydb = connect_to_mysql(mysql_host, mysql_user, mysql_password, mysql_database)
  if not mydb:
    exit(1)

  # Execute query and fetch results
  data = execute_query(mydb, sql_query)
  if not data:
    exit(1)

  # Convert results to JSON with float values
  save_to_json(data, filename)