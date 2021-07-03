from __future__ import print_function
from google.cloud import vision
from google.cloud import storage
import os
import configparser
from mysql.connector import connection
import sendMail as mail
import numpy as np
import sqlalchemy as db
import object_loc as obj
from flask import Flask, request, make_response
import datetime
import invokepubsub as ips
# Module For Connecting To MySQL database
import json
import pandas as pd
from google.oauth2 import service_account
import pandas_gbq
from urllib import parse
# initializing Flask app
app = Flask(__name__)
@app.route("/", methods=['GET'])
def write_to_mySQL():
    try:    
           
        json_to_bq={}
        #fetcing the query params 
        filename=parse.parse_qs(parse.urlparse(request.url).query)['filename'][0]
        print(filename)
        Config = configparser.ConfigParser()
        Config.read('config.ini')
        #setting service account json file
        json_file_name = Config.get('GENERAL', 'json_file_name')
        # setting mail variables
        email=Config.get('email','email_id')
        password=Config.get('email','password')
        email_to=Config.get('mail','to')
        #mail.sendemail(email, email_to, '', '','Label Detection From Images - daily run started for date range','', '', 'smtp.gmail.com',password, 587)
        
        # setting database credentials 
        db_password =Config.get('DATABASE','db_password')
        public_ipaddress = Config.get('DATABASE','public_ipaddress')
        db_name = Config.get('DATABASE','db_name')
        project_id = Config.get('DATABASE','project_id')
        instance_name = Config.get('DATABASE','instance_name') 
        #setting the google credentials with service account json file 
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=json_file_name
        bucket_name=Config.get('GCS', 'bucket_name')
        # setting the pub sub topic id
        topic_id=Config.get('pubsub','topic_id')
        my_conn = db.create_engine("mysql+mysqldb://root:"+db_password+"@"+public_ipaddress+"/"+db_name)
        #getting the metadata
        metadata=db.MetaData()
        #getting the table
        labels = db.Table('labels', metadata, autoload=True, autoload_with=my_conn)
        # getting the latest successful timestamp from big query
        sql = """
            SELECT time_stamp,execution_summary FROM `hackathon-project-318305.hackathon_Dataset.logs` order by time_stamp desc LIMIT 1
            """
        credentials = service_account.Credentials.from_service_account_file(
                json_file_name,
            )
        #getting the datafraame from big query
        df = pandas_gbq.read_gbq(sql, project_id=project_id, credentials=credentials)
        # declaration of time_stamp variable for pulling out latest data
        # last_time_stamp=datetime.timedelta(days=2)

        # if df['time_stamp']==datetime.timedelta(days=2) and df['execution_summary']=='SUCCESS':
        #      last_time_stamp=datetime.timedelta(days=1)

        #getting the object annotations from functions
        obj.get_object_annotations(my_conn,filename)
        #get GCS bucket 
        storage_client=storage.Client()
        bucket=storage_client.bucket(bucket_name)
        image_paths=[]
        #get GCS object names 
        if filename!='':
            image_paths.append("gs://"+bucket_name+"/"+filename)
        else:    
            for blob in list(bucket.list_blobs()):
                image_paths.append("gs://"+bucket_name+"/"+blob.name)
                #new_name= datetime.delta(days=1) + blob.name.split("/")[1]
                #new_blob = bucket.rename_blob(blob, new_name)
        print("image_paths",image_paths)
        label_output = []
        client = vision.ImageAnnotatorClient()
        image = vision.Image()
        json_array=[]
        for image_path in image_paths:
            image.source.image_uri = image_path
            response = client.label_detection(image=image)
        
            for label in response.label_annotations:
                json_for_label={}
                #print(label.description, '(%.2f%%)' % (label.score*100.))
                banner_in_db=my_conn.execute(db.select([labels.columns.banner_name]).
                where(labels.columns.banner_name==image_path[30:])).fetchall()
                
                if len(banner_in_db)==0:
                    json_for_label['label_name']=label.description
                    json_for_label['label_percentage']=str(label.score*100) + "%"
                    json_for_label['banner_name']=image_path[30:]
                    json_array.append(json_for_label)
                else:
                    print("this record already exists")
        #creating a log in big query
        json_to_bq["execution_summary"] = "SUCCESS"
        json_to_bq["output_labels"] = json_array
        json_to_bq["time_stamp"]=datetime.datetime.now().isoformat()
        
        #print(json_array)
        df = pd.DataFrame(json_array, columns=['label_name', "label_percentage", "banner_name"])
        #pushing the data to My SQL database
        df.to_sql(con=my_conn,name='labels',if_exists='append',index=False)
        print("table labels created")
        #mail.sendemail(email, email_to, '', '','Label Detection From Image - daily run completed for date range','', '', 'smtp.gmail.com',password, 587)
        try:
            ips.main(json.dumps(json_to_bq),json_file_name,project_id,topic_id)
        except Exception as e_writing_to_bq:
            print("ERROR : while writing data to bq using PUB_SUB .Error message is "+str(e_writing_to_bq))
    except Exception as exp_main:
        #mail.sendemail(email, email_to, '', '','Label Detection From Image - daily run stopped due to error ' + str(exp_main),'', '', 'smtp.gmail.com', password, 587)
        print('Script stopped due to some error', str(exp_main))
        #creating log in big query
        json_to_bq["execution_summary"] = "Failed with error: " + str(exp_main)
        json_to_bq["output_labels"] = "Error"
        json_to_bq["time_stamp"]=datetime.datetime.now().isoformat()
        try:
            ips.main(json.dumps(json_to_bq),json_file_name,project_id,topic_id)
        except Exception as e_writing_to_bq:
            print("ERROR : while writing data to bq using PUB_SUB .Error message is "+str(e_writing_to_bq))

        return (str(exp_main))
    return ("data extracted successfully")
if __name__ == '__main__':
		PORT = int(os.getenv('PORT')) if os.getenv('PORT') else 8080
	
		# This is used when running locally. Gunicorn is used to run the
		# application on Cloud Run. See entrypoint in Dockerfile.
		app.run(host='127.0.0.1', port=PORT, debug=True)

