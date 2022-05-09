# 
# Modules
# 
import re
import boto3
from exif import Image

# Start s3 client using boto3
s3 = boto3.client('s3')

# Main functino
def lambda_handler(event, context):
    
    # for each file that exists in the s3 bucket
    for record in event['Records']:
        try:

            # Get file information
            key = record['s3']['object']['key'] # file name

            # Get inbound bucket information
            bucket = record['s3']['bucket']['name'] # bucket name where original files will be uploaded
            print("### Detected upload in bucket: ", bucket)
            match_input_bucket = re.match("^input-image-",bucket)
            
            if not match_input_bucket:
                print("### Input bucket name is wrong. Please follow the template 'input-image-' in your bucket name.")
                print("### Exit")
                exit(0)
            
            # Get outbound bucket information
            response = s3.list_buckets()
            _destination_bucket_exists = 0
            for buckets in response['Buckets']:
                print("### Detected bucket name in AWS Account: ", buckets["Name"])
                match_output_bucket = re.match("^output-image-",buckets["Name"])
                if match_output_bucket:
                    print("### Matched upload bucket name: ", buckets["Name"])
                    bucket_upload = buckets["Name"] # bucket name to store cleaned images
                    _destination_bucket_exists = 1
            
            if _destination_bucket_exists == 0:
                print("### Output bucket not found. Please create a new bucket and follow the template 'output-image-' in the bucket name.")
                print("### Exit")
                exit(0)

            download_path = '/tmp/' + key # Where the temporary files will be stored in Lambda environment
            upload_path = 'cleaned-{}'.format(key) # clean file name
            
            # Some logs
            print("### STARTING SOURCE FILE DOWNLOAD ###")
            print("###    BUCKET: " + bucket)
            print("###    FILE  : " + key)
            print("###    D/L AS: " + download_path)
            
            # Download file from source bucket
            s3.download_file(bucket, key, download_path)
            
            #
            # Gathering METADATA from file
            #
            folder_path = '/tmp'
            img_filename = key
            img_path = f'{folder_path}/{img_filename}'

            # Open source image
            with open(img_path, 'rb') as img_file:
                img = Image(img_file)

            # Print metadata in logs
            try:
                for md in img.list_all():
                    try:
                        print("--> " + md + "\t\tValue: " + str(img.get(md)))     
                    except Exception as fh:
                        print(fh)
            except Exception as err:
                print(err)

            # Start cleaning the image file
            print("### STARTING MEDATADA REMOVAL ###")
            for md in img.list_all():
                img.delete(md)
                print("--> " + md + "\t\tValor: " + str(img.get(md)))
            
            # SAving file without its metadata
            print("### SAVING CLEAN FILE IN /TMP")
            with open(f'/tmp/cleaned-{img_filename}', 'wb') as new_image_file:
                    new_image_file.write(img.get_file())

            # Uploading clean image to bucket
            print("### UPLOADING CLEANED FILE... ###")
            print("###    BUCKET: " + bucket_upload)
            print("###    FILE  : " + key)
            print("###    U/L AS: " + f'/tmp/cleaned-{img_filename}')

            s3.upload_file(f'/tmp/cleaned-{img_filename}', bucket_upload, upload_path)
        
            print("### THE LAMBDA HAS EXECUTED SUCCESSFULLY. PLEASE CHECK YOUR DESTINATION BUCKET")
        except Exception as e:
            print(e)
