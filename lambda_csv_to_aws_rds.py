import boto3
import pymysql

bucket_name = 'BUCKET'
csv_filename = 'FILENAME.csv'

db_tablename = "DB_TABLENAME"
db_name = "DB_NAME" # RDS MySQL DB name

def read_data():
	s3_client = boto3.client('s3',
							aws_access_key_id="KEY",
							aws_secret_access_key="SECRET_KEY"
		)

	csv_obj = s3_client.get_object(Bucket=bucket_name, Key=csv_filename)
	content = csv_obj['Body']
	csv_string = content.read().decode('utf-8').split("\n")
	return csv_string


def table_exists(con, table_name):

	conn = None
	rds_host = 'DB_NAME.NNN.AWS_REGION.rds.amazonaws.com'
	username = 'admin'
	password = ''
	database = 'DB_NAME'

	#try:
	connection = pymysql.connect(host=rds_host, 
	    						user=username, 
	    						passwd=password, 
	    						db=db_name
	)
	
    cur = con.cursor()
    cur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(table_name))
    if cur.fetchone()[0] == 1:
        cur.close()
        return True

    cur.close()
    return False


def lambda_csv_to_aws_rds(connection):
	with connection:
	    # 1. create the db table if it does not exist yet
	    cur = connection.cursor()
	    if not table_exists(connection, db_tablename):
	    	query = "create table {} ( id INT NOT NULL AUTO_INCREMENT, Name varchar(255) NOT NULL, Age INT, PRIMARY KEY (id))".format(db_tablename)
	    	cur.execute(query)
	    	connection.commit()
	    	print("Created a {} table".format(db_tablename))

	    # 2. read the csv file line by line and insert in the db
	    with connection.cursor() as cur:
	        for passenger in csv_string: # Iterate over S3 csv file content and insert into MySQL database
	            try:
	                passenger = passenger.replace("\n","").split(",")
	                print (">>>>>>>"+str(passenger))
	                name = passenger[3]
	                age = passenger[5]
	                cur.execute('insert into {} (Name) values("'+str({})+'")'.format(db_tablename, name))
	                cur.execute('insert into {} (Age) values("'+str({})+'")'.format(db_tablename, age))
	            except:
	                continue
	        
	        if conn:
	        	conn.commit()
	        
	        cur.execute("select count(*) from {}".format(db_tablename))
	        print ("Total records on DB :"+str(cur.fetchall()[0]))
