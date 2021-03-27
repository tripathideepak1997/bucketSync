import os
import subprocess
import sys
import uuid
import shutil
from concurrent.futures import ThreadPoolExecutor
import argparse
import boto3
from pprint import pprint
from pathlib import Path
from collections import namedtuple


LIMIT = 1000  # Number of files
FILTER = ""   # Filter is used for wild card filtering like your file containes name 'cs' in abcssds.csv  then this file will be filtered
key = 'aws_access_key_id'
secret = 'aws_secret_access_key'

ENDPOINT_URL = "http://127.0.0.1:9000/"  # Minio Endpoint Url goes here


Minio_Cred = ["minioadmin", "minioadmin"] # Store s3 credentials here




# Store the environment here to be used in function call you can use different environment like {production : Production_Cred}

Cred = {
    'minio': Minio_Cred
}





def execute(cmd):
    popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
    for stdout_line in iter(popen.stdout.readline, ""):
        yield stdout_line
    popen.stdout.close()
    return_code = popen.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, cmd)


def get_temp_folder_name():
    return str(uuid.uuid4()).replace('-','')


def create_folder():
    current_location = os.getcwd()
    dir_path = get_temp_folder_name()
    new_folder_path = os.path.join(current_location, dir_path)

    if not os.path.exists(new_folder_path):
        os.mkdir(new_folder_path)

    return new_folder_path
    # if os.path.exists('')

def set_env(env):
    print(env)
    os.environ[key] = Cred.get(env)[0]
    os.environ[secret] = Cred.get(env)[1]


def copy_one_to_another(to_bucket, from_bucket,from_="production", to="staging", endpoint_url_from="", endpoint_url_to="", from_prefix = "", to_prefix="", s5cmd = True):
    new_dir = create_folder()
    
    
    if s5cmd:
        print(to_bucket, from_bucket, from_, to)
        os.chdir(new_dir)
        set_env(from_)


        cmd = f's5cmd {"--endpoint-url " + endpoint_url_from if endpoint_url_from else "" } cp s3://{from_bucket}/{from_prefix}  .'

        for path in execute(cmd.split()):
            print(path, end="")
        

        set_env(to)
        if not to_prefix:
            cmd = f's5cmd {"--endpoint-url " + endpoint_url_to if endpoint_url_to else "" } cp . s3://{to_bucket}/'
        else:
            cmd = f's5cmd {"--endpoint-url " + endpoint_url_to if endpoint_url_to else "" } cp . s3://{to_bucket}/{to_prefix}'

        for path in execute(cmd.split()):
                print(path, end="")
    else:
        if endpoint_url_from:
            s3_client1 = client = boto3.resource('s3', aws_access_key_id=Cred[from_][0], aws_secret_access_key=Cred[from_][1], endpoint_url=endpoint_url_from)
        else:
            s3_client1 = client = boto3.resource('s3', aws_access_key_id=Cred[from_][0], aws_secret_access_key=Cred[from_][1])

        if endpoint_url_to:
            s3_client2 = client = boto3.resource('s3', aws_access_key_id=Cred[to][0], aws_secret_access_key=Cred[to][1], endpoint_url=endpoint_url_to)
        else:
            s3_client2 = client = boto3.resource('s3', aws_access_key_id=Cred[to][0], aws_secret_access_key=Cred[to][1])
        
        file_list = list_bucket(from_bucket, env=from_, endpoint_url=endpoint_url_from, suffix=from_prefix, s5cmd=s5cmd)
        for file in file_list.keys():
            file_name = file.split("/")[-1]
            if not FILTER in file_name:
                continue
            file_dir = "/".join([new_dir] + file.split("/")[:-1])
            Path(file_dir).mkdir(parents=True, exist_ok=True)
            with open(f"{file_dir}/{file_name}", 'w') as f:
                s3_client1.meta.client.download_file(from_bucket, file, f"{file_dir}/{file_name}")

        set_env(to)

        cmd = f'aws s3 --profile {to} sync {new_dir}/ {"--endpoint-url " + endpoint_url_to if endpoint_url_to else "" } s3://{to_bucket}/ '
        try:
            for path in execute(cmd.split()):
                print(path, end="")
        except Exception as err:
            print(str(err))


    shutil.rmtree(new_dir)


def list_bucket(bucket, env="minio", endpoint_url="", suffix="", s5cmd = False):
    file_name = dict()
    
    if s5cmd:
        set_env(env)
        cmd = f's5cmd  {"--endpoint-url " + endpoint_url if endpoint_url else "" }  ls s3://{bucket}/{suffix}'
        for path in execute(cmd.split()):
            print(path, end="")
    else:
        if endpoint_url:
            client = boto3.client('s3', aws_access_key_id=Cred[env][0], aws_secret_access_key=Cred[env][1], endpoint_url=endpoint_url)
        else:
            client = boto3.client('s3', aws_access_key_id=Cred[env][0], aws_secret_access_key=Cred[env][1])
        response = client.list_objects_v2(Bucket=bucket, MaxKeys=LIMIT, Prefix=suffix)
        # import pdb; pdb.set_trace()
        file_name.update({resp['Key']: [resp['LastModified'], resp['Size']] for resp in response.get('Contents',[]) if resp})
        pprint(file_name)

        while response['IsTruncated'] and len(file_name) < LIMIT:
            response = client.list_objects_v2(Bucket=bucket, ContinuationToken=response['NextContinuationToken'])
            file_name.update({resp['Key']: resp['LastModified'] for resp in response['Contents']})
            pprint(file_name)

    
    return file_name



copy_one_to_another(to_bucket='dest-bucket-name', from_bucket='source-bucket-name', s5cmd=False,from_="minio", to="minio", endpoint_url_to=ENDPOINT_URL,from_prefix="prefix goes here")


