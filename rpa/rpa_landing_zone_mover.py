import boto3
import tempfile
from zipfile import ZipFile
from io import BytesIO
# from notification import slack
import json
import typing
import datetime
from time import sleep


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
    print('destination path will be', destination_path)

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

                        for line in manifest.readlines():
                            jdoc = json.loads(line)
                            if not crawler_used:
                                crawler_used = jdoc['crawler_used']
                            if jdoc.get('entry_type', None):
                                # reading through all of the manifest to get the metadata lines... :/
                                corrected_manifest_jdocs.append(jdoc)

                except Exception as e:
                    msg = f"[ERROR] RPA Landing Zone mover failed to handle manifest file: {source_bucket}/{s3_obj.key} > {zip_filename} \n{e}"
                    slack.send_notification(msg)

                if not crawler_used:
                    msg = f"[ERROR] RPA Landing Zone mover failed to discover crawler_used: {source_bucket}/{s3_obj.key} > {zip_filename}"
                    slack.send_notification(msg)
                    exit(1)

                previous_hashes, cmltv_manifest_s3_obj, cmltv_manifest_lines = get_previous_manifest_for_crawler(
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
                            upload_file_from_zip(zf, zip_filename)
                            upload_file_from_zip(zf, to_move_meta)

                except Exception as e:
                    print('upload new file err', type(e), e)

                upload_file_from_zip(zf, f'{base_dir}crawler_output.json')
                upload_jsonlines(corrected_manifest_jdocs, 'manifest.json')

                print('after uploading files')

                # the manifest from rpa is everything because it can't determine previous hashes before downloading
                # need to make a corrected one from filtered hashes
                # dont copy the existing one like this:: upload_new_file(zf, f'{base_dir}manifest.json')

                with tempfile.TemporaryFile(mode='r+') as new_manifest:
                    if cmltv_manifest_s3_obj:
                        try:
                            # copy old cumulative manifest lines into new manifest file
                            for jdoc in cmltv_manifest_lines:
                                line = json.dumps(jdoc) + '\n'
                                new_manifest.write(line)
                        except Exception as e:
                            print(
                                'error writing jdocs from cmltv_manifest_lines to new_manifest', type(e), e)

                        try:
                            # copy old cumulative manifest to a new name before overwriting
                            ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                            backup_filename = f'cumulative-manifest.{ts}.json'
                            print('backup filename', backup_filename)
                            sleep(5)
                        except Exception as e:
                            print('backup_filename e', type(e), e)

                        try:
                            backup_prefix = get_cumulative_manifest_prefix(crawler_used).replace(
                                'cumulative-manifest.json', backup_filename)
                            # copy old with timestamped name
                            print('cmltv_manifest_s3_obj copy obj', 'Key', cmltv_manifest_s3_obj.key,
                                  'Bucket', cmltv_manifest_s3_obj.bucket_name)
                            sleep(5)
                        except Exception as e:
                            print('backup_prefix e', type(e), e)

                        try:
                            s3.Object(destination_bucket, backup_prefix).copy_from(
                                CopySource={'Key': cmltv_manifest_s3_obj.key,
                                            'Bucket': cmltv_manifest_s3_obj.bucket_name}
                            )
                        except Exception as e:
                            print('upload new cumulative manifest error', type(e), e)
                            # TODO: fix this error
                            # upload new cumulative manifest error <class 'TypeError'> Unicode-objects must be encoded before hashing

                    try:
                        for jdoc in corrected_manifest_jdocs:
                            line = json.dumps(jdoc) + '\n'
                            new_manifest.write(line)
                    except Exception as e:
                        print('err in json dump corrected_manifest_jdocs', type(e), e)

                    print('after writing new manifest', new_manifest)
                    print('was there a cumulative manifest?',
                          cmltv_manifest_s3_obj)

                    # rewind file to top so there is data to put
                    new_manifest.seek(0)

                    try:
                        # upload new
                        key = get_cumulative_manifest_prefix(crawler_used)
                        print('upload new cumulative manifest',
                              destination_bucket, key)
                        s3_client.put_object(
                            Body=new_manifest,
                            Bucket=destination_bucket,
                            Key=key
                        )
                    except Exception as e:
                        # TODO: fix error
                        # upload new cumulative manifest error <class 'TypeError'> Unicode-objects must be encoded before hashing
                        print('upload new cumulative manifest error', type(e), e)

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
    lines = None
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
        return (previous_hashes, manifest_s3_obj, lines)


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


def upload_file_from_zip(zf_ref, zip_filename, bucket=destination_bucket, prefix=destination_prefix_dt):
    try:
        with zf_ref.open(zip_filename, "r") as f:
            _, __, filename = zip_filename.rpartition('/')
            print('upload file obj', f, bucket, f"{prefix}/{filename}")
            s3_client.upload_fileobj(
                f, bucket, f"{prefix}/{filename}")
    except Exception as e:
        print('Error upload_file_from_zip', type(e), e)
        pass


def upload_jsonlines(lines, filename, bucket=destination_bucket, prefix=destination_prefix_dt):
    with tempfile.TemporaryFile(mode='r+') as new_file:
        for line in lines:
            jdoc = json.dumps(line) + '\n'
            new_file.write(jdoc)

        s3_client.upload_fileobj(new_file, bucket, f"{prefix}/{filename}")


if __name__ == '__main__':
    filter_and_move()
