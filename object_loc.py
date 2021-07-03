from google.cloud import vision
import os
import configparser
from google.cloud import storage
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy as db
"""Localize objects in the image on Google Cloud Storage

Args:
uri: The path to the file in Google Cloud Storage (gs://...)
"""
def get_object_annotations(my_conn,filename=''):
    Config = configparser.ConfigParser()
    Config.read('config.ini')
    json_file_name = Config.get('GENERAL', 'json_file_name')
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=json_file_name
    bucket_name=Config.get('GCS', 'bucket_name')
    #get GCS bucket 
    storage_client=storage.Client()
    bucket=storage_client.bucket(bucket_name)
    client = vision.ImageAnnotatorClient()
    image_paths=[]
        #get GCS object names 
    if filename!='':
        image_paths.append("gs://"+bucket_name+"/"+filename)
    else:
        for blob in list(bucket.list_blobs()):
            image_paths.append("gs://"+bucket_name+"/"+blob.name)
    json_for_obj_array=[]
    for image_path in image_paths:
        image = vision.Image()
        image.source.image_uri = image_path
        objects = client.object_localization(
        image=image).localized_object_annotations
        print('Number of objects found: {}'.format(len(objects)))
        #getting the metadata
        metadata=db.MetaData()
        #getting the table
        objects_table = db.Table('objects', metadata, autoload=True, autoload_with=my_conn)
        if len(objects) !=0:
            for object_ in objects:
                json_for_obj={}
                obj_in_db=my_conn.execute(db.select([objects_table.columns.banner_name]).
                where(objects_table.columns.banner_name==image_path[30:])).fetchall()
                if len(obj_in_db)==0:
                    json_for_obj['banner_name'] = image_path[30:]
                    json_for_obj['object_name'] = object_.name
                    json_for_obj['object_percentage']=str(object_.score*100) + "%" 
                    json_for_obj_array.append(json_for_obj)
                else:
                    print("this record already exists")
    df = pd.DataFrame(json_for_obj_array, columns=["object_name", "object_percentage", "banner_name"])
    print("Dataframe",df)
    df.to_sql(con=my_conn,name='objects',if_exists='append',index=False)
    print("table  Objects created")
    print(json_for_obj_array)
        #print('\n{} (confidence: {})'.format(object_.name, object_.score))
#print(json_for_obj_array)
      