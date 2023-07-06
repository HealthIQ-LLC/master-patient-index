import boto3
from cryptography.fernet import Fernet

from .logging import DEBUG_ROUTE, version

NAME = ''
KEY_NAME = f'empi_{version}.key'
BUCKET_NAME = f'{NAME}/{version}'
ROLE_ARN = ''
ROLE_SESSION_NAME = ''


class S3Session:
    """
    Manage access to reads & writes on an S3 bucket
    """
    def __init__(self):
        session = boto3.session.Session()
        sts_client = session.client('sts')
        role_object = sts_client.assume_role(
            RoleArn=ROLE_ARN,
            RoleSessionName=ROLE_SESSION_NAME)
        self.resource = session.resource(
            's3', 
            aws_access_key_id=role_object['Credentials']['AccessKeyId'],
            aws_secret_access_key=role_object['Credentials']['SecretAccessKey'],
            aws_session_token=role_object['Credentials']['SessionToken'], 
        )
        self.bucket = self.resource.Bucket(name=BUCKET_NAME)


class EMPIEncryptor:
    """
    Create and manage a Fernet key, encrypt and decrypt files sharing the key.
    """
    def __init__(self, s3_session):
        self.s3_session = s3_session

    @staticmethod
    def key_create():
        key = Fernet.generate_key()
        return key

    def key_write(self, key, key_name):
        with open(key_name, 'wb') as mykey:
            mykey.write(key)
        key_object = self.s3_session.Object(
            self.s3_session.bucket, 
            key=key_name
        )
        key_object.upload_file(key_name)

    def key_load(self, key_name):
        key_object = self.s3_session.Object(
            self.s3_session.bucket, 
            key=key_name
        )
        key_object.download_file(f'/')
        with open(key_name, 'rb') as mykey:
            key = mykey.read()

        return key

    def file_encrypt(self, key, file_name):
        f = Fernet(key)
        with open(file_name, 'rb') as file:
            original = file.read()
        encrypted = f.encrypt(original)
        encrypted_file_name = f'_enc_{file_name}'
        with open(encrypted_file_name, 'wb') as file:
            file.write(encrypted)
        encrypted_object = self.s3_session.Object(
            self.s3_session.bucket, 
            key=encrypted_file_name
        )
        encrypted_object.upload_file(
            encrypted_file_name, 
            ExtraArgs={'StorageClass': 'REDUCED_REDUNDANCY'}
        )

    def file_decrypt(self, key, file_name):
        encrypted_file_name = f'_enc_{file_name}'
        encrypted_object = self.s3_session.Object(
            self.s3_session.bucket, 
            key=encrypted_file_name
        )
        encrypted_object.download_file(f'/')
        f = Fernet(key)
        with open(encrypted_file_name, 'rb') as file:
            encrypted = file.read()
        
        return f.decrypt(encrypted)


class EMPIFileCrypt:
    """
    This is a context manager which encrypts EMPI metrics, destroys plaintext
    versions, and exposes a special use case method for examining decrypts of
    these files within the context management.
    """

    def __init__(self, file_name, bury=True):
        self.bury = bury
        self.file_name = file_name
        self.encrypted_file_name = f"_enc_{file_name}"
        s3_session = S3Session()
        self.s3_resource = s3_session.resource
        self.s3_bucket = s3_session.bucket
        self.encryptor = EMPIEncryptor(s3_session)
        if not self.check_file(KEY_NAME):
            mykey = self.encryptor.key_create()
            self.encryptor.key_write(mykey, KEY_NAME)

    def check_file(self, file_name):
        try:
            self.s3_resource.Object(self.s3_bucket, file_name).load()
            exists = True
        except:
            exists = False

        return exists

    def __enter__(self):
        self.loaded_key = self.encryptor.key_load(KEY_NAME)
        if self.bury & self.check_file(self.file_name):
            self.encryptor.file_encrypt(self.loaded_key, self.file_name)

    def __exit__(self, e_type, value, traceback):
        if e_type is not None:
            error_msg = f"{e_type} : {value} : {traceback}"
            print(error_msg, file=DEBUG_ROUTE)
        else:
            if self.bury & self.check_file(self.file_name):
                try:
                    self.s3_resource.delete_object(
                        Bucket=self.s3_bucket, 
                        Key=self.file_name
                    )
                except:
                    error_msg = f"Error cleaning up {self.file_name}"
                    print(error_msg, file=DEBUG_ROUTE)

    def access_encrypted_file(self):
        return self.encryptor.file_decrypt(
            self.loaded_key, 
            self.encrypted_file_name
            )

    def decrypt(self):
        decrypt = None
        if self.check_file(self.encrypted_file_name):
            try:
                decrypt = self.access_encrypted_file()
            except:
                error_msg = f"Decrypt error with {self.encrypted_file_name}"
                print(error_msg, file=DEBUG_ROUTE)

        return decrypt


def empi_file_bury(file_name, read_out=False):
    """
    :param file_name: the plaintext EMPI metric file to be handled
    :param read_out: set to True to get out with a file decrypt for analytics
    """
    with EMPIFileCrypt(file_name) as empi_file:
        response = empi_file.encrypted_file_name
        if read_out:
            response = empi_file.decrypt()

    return response


def empi_file_disinter(encrypted_file_name):
    """
    :param encrypted_file_name: the encrypted EMPI metric file to be handled
    """
    file_name = encrypted_file_name.replace('_enc_', '')
    decrypt = None
    if file_name != encrypted_file_name:
        with EMPIFileCrypt(file_name, False) as empi_file:
            decrypt = empi_file.decrypt()

    return decrypt
