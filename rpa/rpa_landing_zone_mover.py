import boto3
import tempfile
from zipfile import ZipFile
from io import BytesIO
# from notification import slack
import json
import typing
import datetime


class slack:
    def send_notification(msg):
        print(msg)


s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

source_bucket = "advana-data-zone"
source_prefix = "bronze/gamechanger/rpa-landing-zone/"

destination_bucket = "advana-data-zone"
destination_prefix = "bronze/gamechanger/external-uploads/crawler-downloader"

source_path = f"{source_bucket}/{source_prefix}"

external_uploads_dt = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
destination_prefix_dt = f"{destination_prefix}/{external_uploads_dt}"
destination_path = f"{destination_bucket}/{destination_prefix_dt}"


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
        # Update crawler status postgres table
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
    for zip_filename, s3_obj in zip_s3_objs.items():
        crawler_used = None
        try:

            # create in memory zip file object
            with create_zip_obj(s3_obj) as zf:
                zip_names = zf.namelist()
                base_dir = zip_names[0]

                print('base dir', base_dir)

                corrected_manifest_jdocs = []

                # get crawler name from manifest file
                try:
                    with zf.open(f'{base_dir}manifest.json') as manifest:
                        # read through all of the manifest to get the metadata lines... :/
                        for line in manifest.readlines():
                            jdoc = json.loads(line)
                            if not crawler_used:
                                crawler_used = jdoc['crawler_used']
                            if jdoc.get('entry_type', None):
                                corrected_manifest_jdocs.append(jdoc)

                except Exception as e:
                    msg = f"[ERROR] RPA Landing Zone mover failed to handle manifest file: {source_bucket}/{s3_obj.key} > {zip_filename} \n{e}"
                    slack.send_notification(msg)

                if not crawler_used:
                    msg = f"[ERROR] RPA Landing Zone mover failed to discover crawler_used: {source_bucket}/{s3_obj.key} > {zip_filename}"
                    slack.send_notification(msg)
                    exit(1)

                previous_hashes, cmltv_manifest_s3_obj = get_previous_manifest_for_crawler(
                    crawler_used)

                # read metadata files and transfer them and main file to the external-uploads bucket if new
                not_in_previous_hashes = set()
                errors = []

                print('len prev hashes', len(previous_hashes))

                for name in zip_names:
                    if name.endswith('.metadata'):
                        try:
                            with zf.open(name) as metadata:
                                jdoc = json.loads(metadata.readline())

                                version_hash = jdoc.get('version_hash', None)
                                if version_hash and not version_hash in previous_hashes:
                                    not_in_previous_hashes.add(name)
                                    corrected_manifest_jdocs.append(jdoc)
                        except Exception as e:
                            errors.append(name)

                print('len errors', len(errors))
                print('len not_in_previous_hashes',
                      len(not_in_previous_hashes))
                try:

                    for to_move_meta in not_in_previous_hashes:
                        zip_filename = to_move_meta.replace('.metadata', '')
                        print('upload', zip_filename)

                        if zip_filename in zip_names:
                            upload_new_file(zf, zip_filename)
                            upload_new_file(zf, to_move_meta)

                except Exception as e:
                    print('upload new file err', e)

                upload_new_file(zf, f'{base_dir}crawler_output.json')

                print('after uploading files')

                # the manifest from rpa is everything because it can't determine previous hashes before downloading
                # need to make a corrected one from filtered hashes
                # dont copy the existing one like this:: upload_new_file(zf, f'{base_dir}manifest.json')

                with tempfile.TemporaryFile() as new_manifest:

                    for jdoc in corrected_manifest_jdocs:
                        try:
                            # new_manifest.write(json.dump(jdoc))
                            # json.dumps(jdoc, new_manifest)
                            line = json.dumps(jdoc).encode() + b'\n'
                            new_manifest.write(line)
                        except Exception as e:
                            print('err in json dump or write newline', e)

                    print('after writing new manifest', new_manifest)
                    print('was there a cumulative manifest?',
                          cmltv_manifest_s3_obj)

                    try:
                        if cmltv_manifest_s3_obj:
                            # copy old cumulative manifest to a new name before overwriting
                            ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                            backup_filename = f'cumulative-manifest.{ts}.json'
                            print('backup filename', backup_filename)
                            backup_prefix = get_cumulative_manifest_prefix(crawler_used).replace(
                                'cumulative-manifest.json', backup_filename)
                            # copy old with timestamped name
                            print('cmltv_manifest_s3_obj copy obj', 'Key', cmltv_manifest_s3_obj.key,
                                  'Bucket', cmltv_manifest_s3_obj.bucket_name)
                            s3.Object(destination_bucket, backup_prefix).copy_from(
                                CopySource={'Key': cmltv_manifest_s3_obj.key,
                                            'Bucket': cmltv_manifest_s3_obj.bucket_name}
                            )
                    except Exception as e:
                        print('upload new cumulative manifest error', e)

                    try:
                        # upload new
                        s3_client.put_object(
                            Body=new_manifest,
                            Bucket=destination_bucket,
                            Key=get_cumulative_manifest_prefix(crawler_used)
                        )
                    except Exception as e:
                        print('upload new cumulative manifest error', e)
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

        print("DONE DONE DONE")


def get_cumulative_manifest_prefix(crawler_used):
    return f"bronze/gamechanger/data-pipelines/orchestration/crawlers_rpa/{crawler_used}/cumulative-manifest.json"


def get_previous_manifest_for_crawler(crawler_used) -> typing.Tuple[set, typing.Union[None, object]]:
    previous_hashes = set()
    manifest_s3_obj = None
    try:
        key = get_cumulative_manifest_prefix(crawler_used)
        s3_obj = s3.Object(source_bucket, key)
        lines = s3_obj.get()['Body'].read().decode(
            'utf-8').splitlines()
        print('lines in previous manifest obj', len(lines))
        if lines:
            for line in lines:
                jdoc = json.loads(line)
                version_hash = jdoc.get('version_hash', None)
                if version_hash:
                    previous_hashes.add(version_hash)

            # only set it if it has lines, would still be an Object otherwise
            manifest_s3_obj = s3_obj

    except Exception as e:
        msg = f"[WARN] No cumulative-manifest found for {crawler_used}, a new one will be created \n {e}"
        slack.send_notification(msg)

    finally:
        return (previous_hashes, manifest_s3_obj)


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


def upload_new_file(zf_ref, zip_filename, bucket=destination_bucket, prefix=destination_prefix_dt):
    try:
        with zf_ref.open(zip_filename, "r") as f:
            _, __, filename = zip_filename.rpartition('/')
            print('upload file obj', f, bucket, f"{prefix}/{filename}")
            s3_client.upload_fileobj(
                f, bucket, f"{prefix}/{filename}")
    except Exception as e:
        print('Error upload_new_file', e)
        pass


if __name__ == '__main__':
    filter_and_move()
