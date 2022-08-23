import pandas as pd
import boto3
import json
import logging

class S3_Functions:
    def __init__(self,credentials:json):
        self.aws_access_key_id=credentials['aws_key']
        self.aws_secret_access_key=credentials['aws_secrect_key']
        self.s3_client = boto3.client('s3',aws_access_key_id=self.aws_access_key_id,
                                      aws_secret_access_key=self.aws_secret_access_key)
        self.s3_resource = boto3.resource('s3',aws_access_key_id=self.aws_access_key_id,
                                        aws_secret_access_key=self.aws_secret_access_key)


    def upload_file_to_s3(self,file_name:str,aws_bucket:str,aws_key:str):
        try:
            self.s3_client.upload_file(Filename=file_name, Bucket=aws_bucket,
            Key=aws_key ,ExtraArgs={'ACL':'bucket-owner-full-control'})
        except:
            logging.error(f"Something wrong uploading file: {file_name} to: {aws_bucket}/{aws_key}")

    def get_aws_path_csv_files(self,aws_bucket:str,aws_key:str):
        try:
            files = list()
            my_bucket = self.s3_resource.Bucket(aws_bucket)
            for object_summary in my_bucket.objects.filter(Prefix=aws_key):
                print(object_summary.key)
                if '.csv' in object_summary.key:
                    files.append(object_summary.key)
            return files
        except:
            logging.error(f"Something wrong searching csv files in path:{aws_bucket}/{aws_key}")

    def get_aws_csv(self,aws_bucket:str,aws_file:str):
        try:
            response = self.s3_client.get_object(Bucket=aws_bucket, Key=aws_file)
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status == 200:
                print(f"Successful S3 get_object response. Status - {status}")
                df = pd.read_csv(response.get("Body"))
                return df
        except:
            logging.error(f"Something wrong downloading df: {aws_bucket}/{aws_file}")

    def delete_s3_file(self,aws_bucket:str,aws_file:str):
        try:
            response = self.s3_resource.Object(Bucket=aws_bucket, Key=aws_file).delete()
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status == 200:
                print(f"Successful delete {aws_file} from S3. Status - {status}")
        except:
            logging.error(f"Something wrong deleting file: {aws_bucket}/{aws_file}")