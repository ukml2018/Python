import ibm_boto3
from ibm_botocore.client import Config
from ibm_botocore.exceptions import ClientError
from datetime import datetime
import subprocess
import os

# set up IBM COS object
config = {
"apikey": "HxP9MclodB8uTAlO7f-FM2Hrx6Uct4gtoYIpy-v-teSk",
"endpoints": "https://cos-service.bluemix.net/endpoints",
"iam_apikey_description": "Auto generated apikey during resource-key operation for Instance - crn:v1:bluemix:public:cloud-object-storage:global:a/562991dfcb0d4665a75bbcdb7618e6db:03228a50-4f4c-4c19-bd30-77153c465245::",
"iam_apikey_name": "auto-generated-apikey-d02fdc5d-8552-4726-9d46-70e457070502",
"iam_role_crn": "crn:v1:bluemix:public:iam::::serviceRole:Writer",
"iam_serviceid_crn": "crn:v1:bluemix:public:iam-identity::a/562991dfcb0d4665a75bbcdb7618e6db::serviceid:ServiceId-fa22d0ce-de0d-46ce-a364-b930cc554e6b",
"resource_instance_id": "crn:v1:bluemix:public:cloud-object-storage:global:a/562991dfcb0d4665a75bbcdb7618e6db:03228a50-4f4c-4c19-bd30-77153c465245::"
}

api_key = config['apikey']
service_instance_id = config['resource_instance_id']
auth_endpoint = 'https://iam.bluemix.net/oidc/token'

service_endpoint = 'https://s3.us-east.objectstorage.softlayer.net'

cos = ibm_boto3.resource('s3',
    ibm_api_key_id=api_key,
    ibm_service_instance_id=service_instance_id,
    ibm_auth_endpoint=auth_endpoint,
    config=Config(signature_version='oauth'),
    endpoint_url=service_endpoint
)

# upload an object in 5mb chunks
def multi_part_upload(bucket_name, item_name, file_path):
    try:
        print("Starting file transfer for {0} to bucket: {1}\n".format(item_name, bucket_name))
        # set 5 MB chunks
        part_size = 1024 * 1024 * 5

        # set threadhold to 15 MB
        file_threshold = 1024 * 1024 * 15

        # set the transfer threshold and chunk size
        transfer_config = ibm_boto3.s3.transfer.TransferConfig(
            multipart_threshold=file_threshold,
            multipart_chunksize=part_size
        )

        # the upload_fileobj method will automatically execute a multi-part upload
        # in 5 MB chunks for all files over 15 MB
        with open(file_path, "rb") as file_data:
            cos.Object(bucket_name, item_name).upload_fileobj(
                Fileobj=file_data,
                Config=transfer_config
            )

        print("Transfer for {0} Complete!\n".format(item_name))
    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to complete multi-part upload: {0}".format(e))

if __name__ == "__main__":

    # run mysqldump to create backup
    today = datetime.strftime(datetime.now(), '%Y-%m-%d')

    curpath = os.getcwd()

    for root, dirs, files in os.walk(curpath):
        for file in files:
            if file.endswith('.log'):
                filename = file[:-4] + "_" + today + ".log"
                print(filename)
                multi_part_upload('32-crlogs', filename, os.path.join(root, file))
                print(file)
    # # run mysqldump to create backup
    # today = datetime.strftime(datetime.now(), '%Y-%m-%d')
    # # filename
    # backup = 'backup_{0}.sql'.format(today)
    # print(backup)
    # # mysqldump command
    # command = "docker exec docker_mysql_1 /usr/bin/mysqldump  -u root --password=crmysql**## \
    #     --single-transaction --skip-lock-tables landing > {0}".format(backup)
    # print(command)
    # # run command and upload to bucket
    # subprocess.call(command, shell=True)
    # multi_part_upload('31-test-crmysqldumps', backup, backup)
    # print('Cleaning up!')
    # # clean-up
    # subprocess.call("rm *.sql", shell=True)



