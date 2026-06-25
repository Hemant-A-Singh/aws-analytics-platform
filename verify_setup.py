from config.settings import aws, redshift, hubspot, mysql, pipeline
import boto3
import pymysql
import redshift_connector

print("="*50)
print("Project setup verification")
print("="*50)

print("\nChecking AWS S3 connection...")
print(f"Region: {aws.AWS_REGION}")
print(f"S3_Bucket: {aws.S3_BUCKET}")
print(f"Redshift_DB: {redshift.REDSHIFT_DATABASE}")
print(f"Environment: {pipeline.ENV}")

#---------------check_S3_Conn-------------------------------------------# 

try:
    s3 = boto3.client(
        "s3", 
        aws_access_key_id = aws.AWS_ACCESS_KEY_ID,
        aws_secret_access_key = aws.AWS_SECRET_ACCESS_KEY,
        region_name = aws.AWS_REGION
    )
    s3.head_bucket(Bucket= aws.S3_BUCKET)
    print(f"S3 Bucket: {aws.S3_BUCKET} reachable")

except Exception as e:
    print(f"S3 Connection Error: {e}")

#-------------------------------------------Check Redshift Conn--------------------------#

try:

    conn = redshift_connector.connect(
        host= redshift.REDSHIFT_HOST,
        database= redshift.REDSHIFT_DATABASE,
        port= redshift.REDSHIFT_PORT,
        user= redshift.REDSHIFT_USER,
        password= redshift.REDSHIFT_PASSWORD
    )

    cursor = conn.cursor

    cursor.execute("select current_database(), current_user;")
    db,user = cursor.fetchone()
    print(f"Connected — DB: {db}, User: {user}")

    cursor.execute("select schema_name from information.schemata")
    
    schemas = [row[0] for row in cursor.fetchall()]
    print(f"Schemas found: {schemas}")

    conn.close()

except Exception as e:
    print(f"Redshift connection error: {e}")

#-------------------------------------------Check MYSQL Conn--------------------------#

try:
    conn = pymysql.connect(
        host=mysql.HOST,
        port=mysql.PORT,
        database=mysql.DATABASE,
        user=mysql.USER,
        password=mysql.PASSWORD,
        connect_timeout=10
    )
    cursor = conn.cursor()
    cursor.execute("SELECT VERSION();")
    version = cursor.fetchone()[0]
    print(f"Connected — MySQL version: {version}")
    conn.close()
except Exception as e:
    print(f"MySQL error: {e}")



#-------------------------------------------Check Hubspot Conn--------------------------#
if hubspot.API_TOKEN and len(hubspot.API_TOKEN) > 10:
    print(f"HubSpot token loaded)")
else:
    print(" HubSpot API token missing or too short")