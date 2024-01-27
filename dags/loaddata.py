
import boto3
import datetime

def upload_scrap_data_to_space(bucket_name, folder_name, file_path,aws_access_key_id,aws_secret_access_key):
    s3_client = boto3.client('s3',
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        )
    current_date = datetime.datetime.today().strftime("%A-%d-%B-%Y")
    
    file_key = f"{folder_name + current_date + file_path.split('/')[-1]}" # Extract the file name

    s3_client.upload_file(file_path, bucket_name, file_key)