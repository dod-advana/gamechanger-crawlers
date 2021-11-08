import botocore.client
import boto3
import tempfile
from zipfile import ZipFile
from io import BytesIO, StringIO
# from notification import slack
import json


class slack:
    def send_notification(msg):
        print(msg)


s3 = boto3.resource('s3')

source_bucket = "advana-data-zone"
source_prefix = "bronze/gamechanger/rpa-landing-zone/"

destination_bucket = "advana-data-zone"
destination_prefix = "bronze/gamechanger/external-uploads/crawler-downloader"

source_path = f"{source_bucket}/{source_prefix}"
destination_path = f"{destination_bucket}/{destination_prefix}"


def filter_and_move():
    TODO = """
        # for each zip in rpa landing zone
        # get file and unzip, read in any metadata file
        # set clone name from it
        # fetch cumulative manifest for that crawler_used if it exists
            # otherwise create an emtpy one
        # load previous_hashes into a set
        # create to_keep dict
        # for each metadata file
            # read in and compare hash to previous_hashes
            # if new, put its hash into to_keep with value of tuple of metadata and pdf file locations
        
        # !! have this part changed in crawling instead... rename manifest file to total_manifest
        # create empty manifest file
        # load total_manifest file
            # compare each line hash to to_keep.keys()
            # if in to_keep, add line to manifest

        # upload all files in to_keep
        # upload manifest

        # rename old cumulative manifest and upload new cumulative manifest to its location

        # if no errors
            # delete zip
        # otherwise try again?




        # update crawler_info if crawler used doesnt exist there
        # Done by ingest
        #### Update crawler status postgres table
        # Crawler download completed - means its sitting in the external-uploads folder
        # Parse in progress
        # Complete
        """

    # initiate s3 client
    # s3 = boto3.resource('s3')

    # s3 = boto3.resource('s3')
    # bucket = s3.Bucket('test-bucket')
    # # Iterates through all the objects, doing the pagination for you. Each obj
    # # is an ObjectSummary, so it doesn't contain the body. You'll need to call
    # # get to get the whole body.
    # for obj in bucket.objects.filter(Prefix=source_path):
    #     key = obj.key
    #     body = obj.get()['Body'].read()

    zip_s3_objs = get_filename_s3_obj_map()
    for filename, s3_obj in zip_s3_objs.items():
        crawler_used = None
        try:
            zip_names = zf.namelist()
            base_dir = zip_names[0]
            # create in memory zip file object
            with create_zip_obj(s3_obj) as zf:
                # get crawler name from manifest file
                try:
                    with zf.open(f'{base_dir}manifest.json') as manifest:
                        jdoc = json.loads(manifest.readline())
                        crawler_used = jdoc['crawler_used']
                        print('\n\n Crawler used was...', crawler_used)
                except Exception as e:
                    msg = f"[ERROR] RPA Landing Zone mover failed to handle manifest file: {source_bucket}/{s3_obj.key} > {filename} \n{e}"
                    slack.send_notification(msg)

                previous_hashes: set = get_previous_manifest_for_crawler(
                    crawler_used)

                # read metadata files and transfer them and main file to the external-uploads bucket if new
                not_in_previous_hashes = []
                errors = []

                for name in zip_names:
                    if name.endswith('.metadata'):
                        try:
                            with zf.open(name) as metadata:
                                jdoc = json.loads(metadata.readline())
                                version_hash = jdoc['version_hash']
                                if not version_hash in previous_hashes:
                                    not_in_previous_hashes.append(name)
                        except Exception as e:
                            errors.append(name)

                a = ['rpa_test/', 'rpa_test/.DS_Store', '__MACOSX/rpa_test/._.DS_Store', 'rpa_test/DHA-PI 1025.01.pdf.metadata', 'rpa_test/DHA-AI 3020-01.pdf', 'rpa_test/DHA-PI 1100.01.pdf', 'rpa_test/DHA-AI 3020-01.pdf.metadata', 'rpa_test/DHA-IP  19-005.pdf', 'rpa_test/DHA-IP  19-004.pdf', 'rpa_test/DHA-IP  19-004.pdf.metadata',
                     'rpa_test/DHA-IP  19-003.pdf.metadata', 'rpa_test/DHA-PI 1100.01.pdf.metadata', 'rpa_test/DHA-IP  19-003.pdf', 'rpa_test/DHA-IP  19-005.pdf.metadata', 'rpa_test/DHA-PI 3201.05.pdf.metadata', 'rpa_test/DHA-PI 4165.01.pdf.metadata', 'rpa_test/DHA-PI 4165.01.pdf', 'rpa_test/DHA-PI 3201.05.pdf', 'rpa_test/DHA-PI 1025.01.pdf', 'rpa_test/manifest.json']

                for to_move_meta in not_in_previous_hashes:
                    filename = to_move_meta.replace('.metadata', '')
                    if filename in zip_names:
                        upload_new_file(zf, filename)
                        upload_new_file(zf, to_move_meta)

                upload_new_file(zf, f'{base_dir}crawler_output.json')
                upload_new_file(zf, f'{base_dir}manifest.json')

                # # namelist retains root structure so files.zip -> files/ , files/thing.txt, files/thing2.txt etc
                # for name in zf.namelist():
                #     # get crawler name from manifest file
                #     if name.endswith('manifest.json'):
                #         try:
                #             with open(name) as manifest:
                #                 jdoc = json.load(manifest.readline())
                #                 # direct check so it will raise error if it doesnt have that key

                # load_previous_manifest_hashes(crawler_used)

        except Exception as e:
            msg = f"[ERROR] RPA Landing Zone mover failed to handle zip file: {source_bucket}/{s3_obj.key} \n{e}"
            slack.send_notification(msg)

    # zf = ZipFile(obj.get()['Body'].read(), 'r')

    # previous_hashes: set = load_previous_manifest_hashes()

    # read through manifest lines
    # compare hash, if in previous then skip
    # otherwise move pdf and metadata to external uploads


def get_previous_manifest_for_crawler(crawler_used) -> set:
    previous_hashes = set()

    try:
        key = f"bronze/gamechanger/data-pipelines/orchestration/crawlers_rpa/{crawler_used}/cumulative-manifest.json"
        manifest_s3_obj = s3.Object(source_bucket, key)
        lines = manifest_s3_obj.get()['Body'].read().decode(
            'utf-8').splitlines()
        for line in lines:
            jdoc = json.loads(line)
            version_hash = jdoc.get('version_hash', None)
            if version_hash:
                previous_hashes.add(version_hash)

    except Exception as e:
        msg = f"[WARN] No cumulative-manifest found for {crawler_used}, a new one will be created"
        slack.send_notification(msg)

    finally:
        return previous_hashes


def get_filename_s3_obj_map() -> list:
    out = {}
    for obj in s3.Bucket(source_bucket).objects.filter(Prefix=source_prefix):
        if obj.key.endswith('.zip'):
            _, __, name_with_ext = obj.key.rpartition('/')
            filename, _, ext = name_with_ext.rpartition('.')
            out[filename] = obj
    # I thought I could use filename here, but the archive keeps the original filename so this one is irrelevant when searching in the zip
    return out


def create_zip_obj(s3_obj) -> ZipFile:
    body_bytes = s3_obj.get()['Body'].read()
    bytes_obj = BytesIO(body_bytes)
    zf = ZipFile(bytes_obj, 'r')
    return zf


def upload_new_file(zf_ref, filename):
    try:
        with zf_ref.open(filename, "rb") as f:
            s3.upload_fileobj(
                f, destination_bucket, destination_path)
    except Exception as e:
        pass


if __name__ == '__main__':
    filter_and_move()
