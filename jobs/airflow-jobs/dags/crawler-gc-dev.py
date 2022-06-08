from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow import XComArg
import os

scanner_image = os.environ["SCANNER_IMAGE"]
crawler_image = os.environ["CRAWLER_IMAGE"]
busybox_image = os.environ["BUSYBOX_IMAGE"]
partition_bucket = os.environ["PARTITION_BUCKET"]
# no leading slash, no trailing slash
partition_directory = os.environ["PARTITION_DIRECTORY"]
scan_concurrency = int(os.environ["SCAN_CONCURRENCY"])
downloads_pvc_name = os.environ["CRAWLER_DOWNLOADS_PVC_NAME"]
files_from_configmap_name = os.environ["FILES_CONFIGMAP_NAME"]
# Airflow dag metadata
args = {
    "owner": "gamechanger",
    "depends_on_past": False,
    "retries": 1,
}

# Configmap that contains files
files_configmap_path = '/app/tmp/configmap-files/'
files_configmap_volume_mount = k8s.V1VolumeMount(name='crawler-schedule-volume',
                                                 mount_path=files_configmap_path,
                                                 sub_path=None,
                                                 read_only=False
                                                 )
files_configmap_volume_config = k8s.V1ConfigMapVolumeSource(
    name=files_from_configmap_name)
files_configmap_volume = k8s.V1Volume(name='crawler-schedule-volume',
                                      config_map=files_configmap_volume_config)

# Downloads/Outputs Volume Mounts
downloads_volume_mount = k8s.V1VolumeMount(name='gc-crawler-downloads', mount_path="/app/tmp/downloads/",
                                           sub_path=None,
                                           read_only=False)
pvc = k8s.V1PersistentVolumeClaimVolumeSource(
    claim_name=downloads_pvc_name)
downloads_volume = k8s.V1Volume(
    name='gc-crawler-downloads', persistent_volume_claim=pvc)


def download_manifest():

    bucket = os.environ["DOWNLOAD_MANIFEST_BUCKET"]
    key = os.environ["DOWNLOAD_MANIFEST_KEY"]
    destination = os.environ["DOWNLOAD_MANIFEST_DESTINATION"]
    # cert_path = os.environ["CACERT_PATH"]

    source_s3 = S3Hook()
    obj = source_s3.get_key(key, bucket)
    obj.download_file(destination)


def backup_manifest(**context):

    bucket = os.environ["DOWNLOAD_MANIFEST_BUCKET"]
    key = os.environ["DOWNLOAD_MANIFEST_KEY"]
    # cert_path = os.environ["CACERT_PATH"]

    source_s3 = S3Hook()

    original_key_count = len(source_s3.list_keys(bucket_name=bucket))
    source_s3.copy_object(source_bucket_key=key, dest_bucket_key=key.split(".json")[0] + str(context['ts_nodash'] + ".json"),
                          source_bucket_name=bucket, dest_bucket_name=bucket)
    current_key_count = len(source_s3.list_keys(bucket_name=bucket))
    if current_key_count <= original_key_count:
        raise Exception("No backup was made.")


def upload_manifest():

    bucket = os.environ["UPLOAD_MANIFEST_BUCKET"]
    key = os.environ["UPLOAD_MANIFEST_KEY"]
    filename = os.environ["UPLOAD_MANIFEST_FILENAME"]
    # cert_path = os.environ["CACERT_PATH"]

    source_s3 = S3Hook()
    # replaces the latest manifest in s3 as well
    source_s3.load_file(filename=filename, key=key,
                        bucket_name=bucket, replace=True)


def skip_scan_if_no_downloads():

    download_dir_to_check = os.environ["GC_CRAWL_DOWNLOAD_OUTPUT_DIR"]
    print((os.listdir(download_dir_to_check)))

    # 2 items being 'lost+found' and 'previous-manifest.json' that we don't need to scan
    if len(os.listdir(download_dir_to_check)) <= 2:
        return "skip_scan"
    else:
        return "purge_s3_partitions"


def combine_s3_partitions_manifests():

    import shutil
    source_s3 = S3Hook()
    output_filepath = os.environ["GC_CRAWL_DOWNLOAD_OUTPUT_DIR"] + \
        "/manifest.json"

    # Read the keys from s3 bucket
    paths = source_s3.list_keys(
        bucket_name=partition_bucket, prefix=partition_directory)

    counter = 0
    # download files from s3 and save to /tmp
    destinations = []
    for key in paths:
        if "manifest.json" in key:
            manifest = source_s3.get_key(key, partition_bucket)
            fp = "/tmp/" + str(counter) + "manifest.json"
            manifest.download_file(fp)
            destinations.append(fp)
            counter += 1

    # combine all manifests into one output file
    with open(output_filepath, 'wb') as outfile:
        for fname in destinations:
            with open(fname, 'rb') as infile:
                shutil.copyfileobj(infile, outfile)
                outfile.write(b"\n")


def partition(lst, n):
    division = len(lst) / float(n)
    return [lst[int(round(division * i)): int(round(division * (i + 1)))] for i in range(n)]


def split_crawler_folder(**kwargs):

    import shutil
    import random

    num_partitions = kwargs["num_partitions"]
    print("Num partitions: " + str(num_partitions))
    download_dir = os.environ["GC_CRAWL_DOWNLOAD_OUTPUT_DIR"]
    prev_manifest_fp = os.environ["GC_CRAWL_PREVIOUS_MANIFEST_LOCATION"]

    # List of files in the download dir
#     data = os.listdir(download_dir)
    data = []
    for entry in os.scandir(download_dir):
        if (entry.is_dir()) | ("previous-manifest.json" in entry.name):
            # skip directories and prev manifest
            continue
        # use entry.path to get the full path of this entry, or use
        # entry.name for the base filename
        data.append(entry.name)

    # make the n subfolders and also create the list that contains dicts of env-vars for each parallel downstream scan task
    subfolder_names = []
    scanner_env_list = []
    for i in range(num_partitions):

        subfolder_path = os.path.join(download_dir, "partition" + str(i) + "/")

        os.makedirs(subfolder_path, exist_ok=True)
        print("Made: " + subfolder_path)
        subfolder_names.append(subfolder_path)
        env_var_dict = {"GC_SCAN_INPUT_PATH": subfolder_path}
        scanner_env_list.append(env_var_dict)

    # shuffle data to normalize the file size per subfolder
    random.shuffle(data)

    split_data = partition(data, num_partitions)

    # each item in the dict will be a foldername : list [] of filenames
    data_list_per_folder = dict(zip(subfolder_names, split_data))
    print(data_list_per_folder)
    # Moving files to subfolders, except for previous-manifest.json
    for folder, file_list in data_list_per_folder.items():
        for f in file_list:
            # skip previous manifest, so that we can process it separately downstream from the download dir instead of a random dir.
            if "previous-manifest.json" in f:
                continue
            shutil.move(download_dir + "/" + f, folder)
            print("Moved: " + f)

    # Return dict of env-vars for the dynamic task mapping

    return scanner_env_list


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def clear_s3_partition_dir():
    source_s3 = S3Hook()
    object_keys = source_s3.list_keys(
        bucket_name=partition_bucket, prefix=partition_directory)

    if object_keys:
        batches = chunks(object_keys, 1000)
        for batch in batches:
            source_s3.delete_objects(bucket=partition_bucket, keys=batch)


def split_crawler_folder_s3(**kwargs):

    import random

    num_partitions = kwargs["num_partitions"]
    print("Num partitions: " + str(num_partitions))
    download_dir = os.environ["GC_CRAWL_DOWNLOAD_OUTPUT_DIR"]
    # prev_manifest_fp = os.environ["GC_CRAWL_PREVIOUS_MANIFEST_LOCATION"]

    # List of files in the download dir
#     data = os.listdir(download_dir)
    data = []
    for entry in os.scandir(download_dir):
        if (entry.is_dir()) | ("previous-manifest.json" in entry.name):
            # skip directories and prev manifest
            continue
        # use entry.path to get the full path of this entry, or use
        # entry.name for the base filename
        data.append(entry.name)

    # make the n subfolders and also create the list that contains dicts of env-vars for each parallel downstream scan task
    subfolder_names = []
    scanner_env_list = []
    for i in range(num_partitions):

        subfolder_path = os.path.join(download_dir, "partition" + str(i) + "/")

        os.makedirs(subfolder_path, exist_ok=True)
        print("Made: " + subfolder_path)
        subfolder_names.append(subfolder_path)
        env_var_dict = {"GC_SCAN_INPUT_PATH": subfolder_path}
        scanner_env_list.append(env_var_dict)

    # shuffle data to normalize the file size per subfolder
    random.shuffle(data)

    split_data = partition(data, num_partitions)

    # each item in the dict will be a foldername : list [] of filenames
    data_list_per_folder = dict(zip(subfolder_names, split_data))
    print(data_list_per_folder)

    source_s3 = S3Hook()
    # Moving files to subfolders, except for previous-manifest.json
    for folder, file_list in data_list_per_folder.items():
        for f in file_list:
            # skip previous-manifest upload
            if "previous-manifest.json" in f:
                continue
            # upload files to s3
            source_s3.load_file(filename=download_dir + "/" + f, key=partition_directory + folder + f,
                                bucket_name=partition_bucket, replace=True)
            # shutil.move(download_dir + "/" + f, folder)
            print("Uploaded to s3 partition: " + f)

    return scanner_env_list

    # DAG definition
dag = DAG(
    dag_id="crawl-parallel-pipeline-gc-dev",
    description="full crawl pipeline",
    default_args=args,
    schedule_interval="0 2 * * 1-5",
    start_date=datetime(2022, 3, 8, 14, 30),
    tags=["crawler", "test"],
)

# Clear the download volume
purge = KubernetesPodOperator(namespace="airflow",
                              image=busybox_image,
                              name="purge-volume",
                              task_id="purge-volume",
                              cmds=['/bin/sh', '-c',
                                    "rm -rf /app/tmp/downloads/*"],
                              get_logs=True,
                              is_delete_operator_pod=True,
                              do_xcom_push=False,
                              volumes=[downloads_volume],
                              volume_mounts=[downloads_volume_mount],
                              dag=dag
                              )

# download previous manifest from s3 to filter out what to download from crawler
download_manifest_task = PythonOperator(
    task_id='copy_from_s3',
    python_callable=download_manifest,
    dag=dag,
    do_xcom_push=False,
    executor_config={
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        volume_mounts=[
                            downloads_volume_mount],
                    )
                ],
                volumes=[downloads_volume],
            )
        )
    })

# Not necessary for a manifest to exist and be downloaded, so this task is just for logging
check_for_manifest_task = BashOperator(
    task_id='check_manifest',
    bash_command="[ -f $GC_CRAWL_PREVIOUS_MANIFEST_LOCATION ] && echo 'Previous manifest exists.' || echo 'Previous manifest does not exist.'",
    dag=dag,
    do_xcom_push=False,
    executor_config={
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        volume_mounts=[downloads_volume_mount],
                    )
                ],
                volumes=[downloads_volume],
            )
        )
    })

# Executor starts a pod that runs the crawler. Configs from the Gamechanger helm deployments from
crawler_to_run = files_configmap_path + \
    datetime.today().strftime('%A').lower() + ".txt"
crawl = KubernetesPodOperator(namespace="airflow",
                              image=crawler_image,
                              name="crawler-task",
                              env_vars={
                                  "GC_CRAWL_SPIDERS_FILE_LOCATION": crawler_to_run,
                                  "GC_CRAWL_CRAWLER_OUTPUT_LOCATION": os.environ["GC_CRAWL_CRAWLER_OUTPUT_LOCATION"],
                                  "GC_CRAWL_DOWNLOAD_OUTPUT_DIR": os.environ["GC_CRAWL_DOWNLOAD_OUTPUT_DIR"],
                                  "GC_CRAWL_PREVIOUS_MANIFEST_LOCATION": os.environ["GC_CRAWL_PREVIOUS_MANIFEST_LOCATION"],
                                  "GC_CRAWL_SKIP_CONFIRMATION": os.environ["GC_CRAWL_SKIP_CONFIRMATION"]},
                              task_id="crawler-task",
                              security_context={'runAsUser': 1000,
                                                'runAsGroup': 1000,
                                                'fsGroup': 1000},
                              get_logs=True,
                              is_delete_operator_pod=True,
                              arguments=["crawl"],
                              volumes=[
                                  files_configmap_volume, downloads_volume],
                              volume_mounts=[
                                  files_configmap_volume_mount, downloads_volume_mount],
                              dag=dag,
                              do_xcom_push=False,
                              )
# Task to branch and stop execution if it checks no files downloaded (ignoring downloaded manifest)
downloads_check = BranchPythonOperator(task_id="downloads-check",
                                       python_callable=skip_scan_if_no_downloads,
                                       dag=dag,
                                       trigger_rule="all_success",
                                       do_xcom_push=False,
                                       executor_config={
                                           "pod_override": k8s.V1Pod(
                                               spec=k8s.V1PodSpec(
                                                   containers=[
                                                       k8s.V1Container(
                                                           name="base",
                                                           volume_mounts=[
                                                               downloads_volume_mount],
                                                       )
                                                   ],
                                                   volumes=[
                                                       downloads_volume],
                                               )
                                           )
                                       })

purge_s3_partitions = PythonOperator(task_id="purge_s3_partitions",
                                     python_callable=clear_s3_partition_dir,
                                     dag=dag)


# Partitioner goes here? Partition a RW-many PV, or upload files to S3 in partitions for downstream tasks.
# Attaches crawler configmap as env
# partition_task = PythonOperator(task_id="partition-data",
#                                 python_callable=split_crawler_folder,
#                                 op_kwargs={"num_partitions": scan_concurrency},
#                                 dag=dag,
#                                 trigger_rule="all_success",
#                                 executor_config={
#                                     "pod_override": k8s.V1Pod(
#                                         spec=k8s.V1PodSpec(
#                                             containers=[
#                                                 k8s.V1Container(
#                                                     name="base",
#                                                     volume_mounts=[
#                                                         downloads_volume_mount],
#                                                     env_from=[crawler_env_from_source
#                                                               ]
#                                                 )
#                                             ],
#                                             volumes=[
#                                                 downloads_volume],
#                                         )
#                                     )
#                                 })


partition_to_s3_task = PythonOperator(task_id="partition-data",
                                      python_callable=split_crawler_folder_s3,
                                      op_kwargs={
                                          "num_partitions": scan_concurrency},
                                      dag=dag,
                                      trigger_rule="all_success",
                                      executor_config={
                                          "pod_override": k8s.V1Pod(
                                              spec=k8s.V1PodSpec(
                                                  containers=[
                                                      k8s.V1Container(
                                                          name="base",
                                                          volume_mounts=[
                                                              downloads_volume_mount],
                                                      )
                                                  ],
                                                  volumes=[
                                                      downloads_volume],
                                              )
                                          )
                                      })

# Run Parallel Scanners
# download s3 partitioned files then scan downloaded crawled files then reupload to s3 with metadata files, then upload individual manifests to partition for downstream task to load and combine
scan_upload = KubernetesPodOperator.partial(namespace="airflow",
                                            image=scanner_image,
                                            name="scanupload-task",
                                            env_vars={"SKIP_S3_UPLOAD": os.environ["SKIP_S3_UPLOAD"],
                                                      "S3_UPLOAD_BASE_PATH": os.environ["S3_UPLOAD_BASE_PATH"],
                                                      "BUCKET": os.environ["BUCKET"],
                                                      "DELETE_AFTER_UPLOAD": os.environ["DELETE_AFTER_UPLOAD"]},
                                            task_id="scanupload-task",
                                            get_logs=True,
                                            is_delete_operator_pod=True,
                                            cmds=["bash", "-c"],
                                            arguments=[
                                                "echo s3://" + partition_bucket + "/" + partition_directory +
                                                "$GC_SCAN_INPUT_PATH && aws s3 cp s3://" + partition_bucket + "/" + partition_directory +
                                                "$GC_SCAN_INPUT_PATH $GC_SCAN_INPUT_PATH --recursive && ls $GC_SCAN_INPUT_PATH && gc scan"],
                                            dag=dag,
                                            do_xcom_push=False,
                                            annotations={
                                                "iam.amazonaws.com/role": "advana/k8s/s3.wildcard"},
                                            ).expand(env_vars=XComArg(partition_to_s3_task))

# combine_manifests = BashOperator(
#     task_id='combine_manifests',
#     bash_command="aws s3 cp s3://" + partition_bucket + "/" + partition_directory +
#     "\"$GC_CRAWL_DOWNLOAD_OUTPUT_DIR\" --exclude '*' --include '*manifest.json' && find \"$GC_CRAWL_DOWNLOAD_OUTPUT_DIR\" -type f -name \"manifest.json\" -exec cat {} + | tee \"$GC_CRAWL_DOWNLOAD_OUTPUT_DIR\"/manifest.json",
#     dag=dag,
#     do_xcom_push=False,
#     executor_config={
#         "pod_override": k8s.V1Pod(
#             metadata=k8s.V1ObjectMeta(
#                 annotations={"iam.amazonaws.com/role": "advana/k8s/s3.wildcard"}),
#             spec=k8s.V1PodSpec(
#                 containers=[
#                     k8s.V1Container(
#                         name="base",
#                         volume_mounts=[downloads_volume_mount],
#                         env_from=[crawler_env_from_source,
#                                   ]
#                     )
#                 ],
#                 volumes=[downloads_volume],
#             )
#         )
#     })

combine_manifests_from_s3 = PythonOperator(
    task_id='combine_manifests_from_s3',
    python_callable=combine_s3_partitions_manifests,
    provide_context=True,
    dag=dag,
    do_xcom_push=False,
    executor_config={
        "pod_override": k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
                annotations={"iam.amazonaws.com/role": "advana/k8s/s3.wildcard"}),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        volume_mounts=[downloads_volume_mount],
                    )
                ],
                volumes=[downloads_volume],
            )
        )
    })


backup_manifest_task = PythonOperator(
    task_id='backup_s3_manifest_with_ts',
    python_callable=backup_manifest,
    provide_context=True,
    dag=dag,
    do_xcom_push=False,
    trigger_rule='none_failed_or_skipped',
    executor_config={
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                    )
                ],
            )
        )

    })

# creates cumulative-manifest.json from previous manifest file if it exists
create_cumulative_manifest = KubernetesPodOperator(namespace="airflow",
                                                   image=busybox_image,
                                                   name="create_cumulative_manifest",
                                                   task_id="create_cumulative_manifest",
                                                   trigger_rule='none_failed_or_skipped',
                                                   env_vars={"GC_CRAWL_PREVIOUS_MANIFEST_LOCATION": os.environ["GC_CRAWL_PREVIOUS_MANIFEST_LOCATION"],
                                                             "GC_CRAWL_DOWNLOAD_OUTPUT_DIR": os.environ["GC_CRAWL_DOWNLOAD_OUTPUT_DIR"]},
                                                   cmds=[
                                                       '/bin/sh'],
                                                   arguments=['-c', "if [ -f \"$GC_CRAWL_PREVIOUS_MANIFEST_LOCATION\" ]; then cat \"$GC_CRAWL_PREVIOUS_MANIFEST_LOCATION\" > \"$GC_CRAWL_DOWNLOAD_OUTPUT_DIR\"/cumulative-manifest.json && echo >> \"$GC_CRAWL_DOWNLOAD_OUTPUT_DIR\"/cumulative-manifest.json; fi"
                                                              ],
                                                   get_logs=True,
                                                   is_delete_operator_pod=True,
                                                   security_context={'runAsUser': 1000,
                                                                     'runAsGroup': 1000,
                                                                     'fsGroup': 1000},
                                                   volumes=[
                                                       downloads_volume],
                                                   volume_mounts=[
                                                       downloads_volume_mount],
                                                   dag=dag,
                                                   do_xcom_push=False
                                                   )

# append the new manifest to cumulative-manifest.json
update_cumulative_manifest = KubernetesPodOperator(namespace="airflow",
                                                   image=busybox_image,
                                                   name="update_cumulative_manifest",
                                                   task_id="update_cumulative_manifest",
                                                   env_vars={"GC_CRAWL_PREVIOUS_MANIFEST_LOCATION": os.environ["GC_CRAWL_PREVIOUS_MANIFEST_LOCATION"],
                                                             "GC_CRAWL_DOWNLOAD_OUTPUT_DIR": os.environ["GC_CRAWL_DOWNLOAD_OUTPUT_DIR"]},
                                                   cmds=[
                                                       '/bin/sh'],
                                                   arguments=['-c', "if [ -f \"$GC_CRAWL_DOWNLOAD_OUTPUT_DIR\"/manifest.json ]; then cat \"$GC_CRAWL_DOWNLOAD_OUTPUT_DIR\"/manifest.json >> \"$GC_CRAWL_DOWNLOAD_OUTPUT_DIR\"/cumulative-manifest.json; fi"
                                                              ],
                                                   get_logs=True,
                                                   is_delete_operator_pod=True,
                                                   trigger_rule='none_failed_or_skipped',
                                                   security_context={'runAsUser': 1000,
                                                                     'runAsGroup': 1000,
                                                                     'fsGroup': 1000},
                                                   volumes=[
                                                       downloads_volume],
                                                   volume_mounts=[
                                                       downloads_volume_mount],
                                                   dag=dag,
                                                   do_xcom_push=False
                                                   )

upload_new_manifest_task = PythonOperator(task_id='upload_new_s3_manifest',
                                          python_callable=upload_manifest,
                                          dag=dag,
                                          do_xcom_push=False,
                                          trigger_rule='none_failed_or_skipped',
                                          executor_config={
                                              "pod_override": k8s.V1Pod(
                                                  spec=k8s.V1PodSpec(
                                                      containers=[
                                                          k8s.V1Container(
                                                              name="base",
                                                              volume_mounts=[
                                                                  downloads_volume_mount],
                                                          )
                                                      ],
                                                      volumes=[
                                                          downloads_volume],
                                                  )
                                              )
                                          })

skip_scan = DummyOperator(task_id='skip_scan', dag=dag)

# need check after scan_upload that files were uploaded
purge >> download_manifest_task >> check_for_manifest_task
check_for_manifest_task >> crawl >> downloads_check
# branching
downloads_check >> purge_s3_partitions >> partition_to_s3_task >> scan_upload >> combine_manifests_from_s3 >> create_cumulative_manifest >> update_cumulative_manifest >> backup_manifest_task >> upload_new_manifest_task
downloads_check >> skip_scan >> create_cumulative_manifest >> update_cumulative_manifest >> backup_manifest_task >> upload_new_manifest_task
