from datetime import datetime
# from fileinput import filename
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
# from airflow.kubernetes.volume import Volume
# from airflow.kubernetes.volume_mount import VolumeMount
from kubernetes.client import models as k8s
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Airflow dag metadata
args = {
    "owner": "gamechanger",
    "depends_on_past": False,
    "retries": 2,
    "catchup_by_default": False,
}

# 3 sections below to create volumes to pass to pods for crawler results
# mounts
results_volume_mount = k8s.V1VolumeMount(name='gc-crawler-output',
                                         mount_path='/app/tmp/output/',
                                         sub_path=None,
                                         read_only=False
                                         )
schedules_volume_mount = k8s.V1VolumeMount(name='crawler-schedule-volume',
                                           mount_path='/app/tmp/crawler-schedules/',
                                           sub_path=None,
                                           read_only=False
                                           )
# volume configs
# results_volume_config = {
#     'persistentVolumeClaim':
#     {
#         'claimName': 'gc-crawler-output'
#     }
# }
results_volume_config = k8s.V1PersistentVolumeClaimVolumeSource(
    claim_name='gc-crawler-output')

# schedule_volume_config = {
#     'configMap':
#     {
#         'name': 'crawler-schedules-configmap'
#     }
# }
schedule_volume_config = k8s.V1ConfigMapVolumeSource(
    name="crawler-schedules-configmap")

# volume objects to attach to pod
results_volume = k8s.V1Volume(name='gc-crawler-output',
                              persistent_volume_claim=results_volume_config)
schedule_volume = k8s.V1Volume(name='crawler-schedule-volume',
                               config_map=schedule_volume_config)

# volume objects to download previous manifest from S3, uses the k8s model since the previous airflow type is deprecated for use with executor_config's pod_override
downloads_volume_mount = k8s.V1VolumeMount(name='gc-crawler-downloads', mount_path="/app/tmp/downloads/",
                                           sub_path=None,
                                           read_only=False)
pvc = k8s.V1PersistentVolumeClaimVolumeSource(
    claim_name='gc-crawler-downloads')
downloads_volume = k8s.V1Volume(
    name='gc-crawler-downloads', persistent_volume_claim=pvc)

# list of configmaps for tasks that use kubernetespodoperator
crawler_configmap_source = k8s.V1ConfigMapEnvSource(
    name='crawler-configmap')
crawler_env_from_source = k8s.V1EnvFromSource(
    config_map_ref=crawler_configmap_source)

scanner_configmap_source = k8s.V1ConfigMapEnvSource(
    name='scanner-uploader-configmap')
scanner_env_from_source = k8s.V1EnvFromSource(
    config_map_ref=scanner_configmap_source)

# configmap/V1EnvFromSource objects in order to mount configmap as env var for certain tasks that require executor_config to launch in pod
# env for upload_manifest task
uploader_configmap_source = k8s.V1ConfigMapEnvSource(
    name='upload-manifest-configmap')
uploader_env_from_source = k8s.V1EnvFromSource(
    config_map_ref=uploader_configmap_source)
# env for download_manifest task
download_configmap_source = k8s.V1ConfigMapEnvSource(
    name='download-manifest-configmap')
download_env_from_source = k8s.V1EnvFromSource(
    config_map_ref=download_configmap_source)
# env for providing the airflow-made aws connection id
aws_configmap_source = k8s.V1ConfigMapEnvSource(
    name='aws-conn-configmap')
aws_env_from_source = k8s.V1EnvFromSource(
    config_map_ref=aws_configmap_source)


def download_manifest():
    import os
    AWS_S3_CONN_ID = os.environ["AWS_S3_CONN_ID"]
    bucket = os.environ["BUCKET"]
    key = os.environ["KEY"]
    destination = os.environ["DESTINATION"]
    source_s3 = S3Hook(AWS_S3_CONN_ID)
    obj = source_s3.get_key(key, bucket)
    obj.download_file(destination)


def backup_manifest(**context):
    import os
    AWS_S3_CONN_ID = os.environ["AWS_S3_CONN_ID"]
    bucket = os.environ["BUCKET"]
    key = os.environ["KEY"]
    source_s3 = S3Hook(AWS_S3_CONN_ID)
    original_key_count = len(source_s3.list_keys(bucket_name=bucket))
    source_s3.copy_object(source_bucket_key=key, dest_bucket_key=key.split(".json")[0] + str(context['ts_nodash']),
                          source_bucket_name=bucket, dest_bucket_name=bucket)
    current_key_count = len(source_s3.list_keys(bucket_name=bucket))
    if current_key_count <= original_key_count:
        raise Exception("No backup was made.")


def upload_manifest():
    import os
    AWS_S3_CONN_ID = os.environ["AWS_S3_CONN_ID"]
    bucket = os.environ["BUCKET"]
    key = os.environ["KEY"]
    filename = os.environ["FILENAME"]
    source_s3 = S3Hook(AWS_S3_CONN_ID)
    # replaces the latest manifest in s3 as well
    source_s3.load_file(filename=filename, key=key,
                        bucket_name=bucket, replace=True)


def skip_scan_if_no_downloads():
    import os
    download_dir_to_check = os.environ["GC_CRAWL_DOWNLOAD_OUTPUT_DIR"]
    print((os.listdir(download_dir_to_check)))

    # 2 items being 'lost+found' and 'previous-manifest.json' that we don't need to scan
    if len(os.listdir(download_dir_to_check)) <= 2:
        return "skip_scan"
    else:
        return "scanupload-task"


# DAG definition
dag = DAG(
    dag_id="crawl-pipeline",
    description="full crawl pipeline",
    default_args=args,
    schedule_interval=None,
    start_date=datetime(2022, 3, 8, 14, 30),
    tags=["crawler", "test"],
)


purge = KubernetesPodOperator(namespace="airflow",
                              image="busybox",
                              name="purge-volume",
                              task_id="purge-volume",
                              cmds=['/bin/sh', '-c',
                                    "find /app/tmp/downloads/ -name '*' -type f -print0 | xargs -0 rm -f"],
                              get_logs=True,
                              is_delete_operator_pod=True,
                              volumes=[downloads_volume],
                              volume_mounts=[downloads_volume_mount],
                              dag=dag
                              )

# download previous manifest from s3 to filter out what to download from crawler
download_manifest_task = PythonOperator(
    task_id='copy_from_s3',
    python_callable=download_manifest,
    dag=dag,
    executor_config={
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        volume_mounts=[downloads_volume_mount],
                        env_from=[download_env_from_source,
                                  aws_env_from_source]
                    )
                ],
                volumes=[downloads_volume],
            )
        )
    })

# Not necessary for a manifest to exist and be downloaded, so this task is just for logging
check_for_manifest_task = BashOperator(
    task_id='check_manifest',
    bash_command="[ -f /app/tmp/downloads/previous-manifest.json ] && echo 'Previous manifest exists.' || echo 'Previous manifest does not exist.'",
    dag=dag,
    executor_config={
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        volume_mounts=[downloads_volume_mount],
                        env_from=[download_env_from_source,
                                  ]
                    )
                ],
                volumes=[downloads_volume],
            )
        )
    })

# Executor starts a pod that runs the crawler. Configs from the Gamechanger helm deployments from
crawl = KubernetesPodOperator(namespace="airflow",
                              image="092912502985.dkr.ecr.us-east-1.amazonaws.com/gc-crawler-downloader-test:latest",
                              name="crawler-task",
                              env_from=[crawler_env_from_source],
                              task_id="crawler-task",
                              security_context={'runAsUser': 1000,
                                                'runAsGroup': 1000,
                                                'fsGroup': 1000},
                              get_logs=True,
                              is_delete_operator_pod=True,
                              arguments=["crawl"],
                              volumes=[
                                  results_volume, schedule_volume, downloads_volume],
                              volume_mounts=[
                                  results_volume_mount, schedules_volume_mount, downloads_volume_mount],
                              dag=dag
                              )


# Task to branch and stop execution if it checks no files downloaded (ignoring downloaded manifest)
downloads_check = BranchPythonOperator(task_id="downloads-check",
                                       python_callable=skip_scan_if_no_downloads,
                                       dag=dag,
                                       executor_config={
                                           "pod_override": k8s.V1Pod(
                                               spec=k8s.V1PodSpec(
                                                   containers=[
                                                       k8s.V1Container(
                                                           name="base",
                                                           volume_mounts=[
                                                               downloads_volume_mount],
                                                           env_from=[crawler_env_from_source
                                                                     ]
                                                       )
                                                   ],
                                                   volumes=[
                                                       downloads_volume],
                                               )
                                           )
                                       })

# scan downloaded crawled files then upload to s3
scan_upload = KubernetesPodOperator(namespace="airflow",
                                    image="092912502985.dkr.ecr.us-east-1.amazonaws.com/gc-crawler-downloader-test:latest",
                                    name="scanupload-task",
                                    env_from=[scanner_env_from_source],
                                    task_id="scanupload-task",
                                    get_logs=True,
                                    is_delete_operator_pod=True,
                                    arguments=['scan'],
                                    annotations={
                                         'iam.amazonaws.com/role': 'advana/k8s/s3.wildcard'},
                                    volumes=[
                                        downloads_volume],
                                    volume_mounts=[
                                        downloads_volume_mount],
                                    dag=dag
                                    )

backup_manifest_task = PythonOperator(
    task_id='backup_s3_manifest_with_ts',
    python_callable=backup_manifest,
    provide_context=True,
    dag=dag,
    trigger_rule='none_failed_or_skipped',
    executor_config={
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        env_from=[
                            download_env_from_source, aws_env_from_source]
                    )
                ],
            )
        )
    })

# creates cumulative-manifest.json from previous manifest file if it exists
create_cumulative_manifest = KubernetesPodOperator(namespace="airflow",
                                                   image="busybox",
                                                   name="create_cumulative_manifest",
                                                   task_id="create_cumulative_manifest",
                                                   trigger_rule='none_failed_or_skipped',
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
                                                   env_from=[
                                                       crawler_env_from_source],
                                                   dag=dag
                                                   )

# append the new manifest to cumulative-manifest.json
update_cumulative_manifest = KubernetesPodOperator(namespace="airflow",
                                                   image="busybox",
                                                   name="update_cumulative_manifest",
                                                   task_id="update_cumulative_manifest",
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
                                                   env_from=[
                                                       crawler_env_from_source],
                                                   dag=dag
                                                   )

upload_new_manifest_task = PythonOperator(task_id='upload_new_s3_manifest',
                                          python_callable=upload_manifest,
                                          dag=dag,
                                          trigger_rule='none_failed_or_skipped',
                                          executor_config={
                                              "pod_override": k8s.V1Pod(
                                                  spec=k8s.V1PodSpec(
                                                      containers=[
                                                          k8s.V1Container(
                                                              name="base",
                                                              volume_mounts=[
                                                                  downloads_volume_mount],
                                                              env_from=[
                                                                  uploader_env_from_source, aws_env_from_source]
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
downloads_check >> scan_upload >> create_cumulative_manifest >> update_cumulative_manifest >> backup_manifest_task >> upload_new_manifest_task
downloads_check >> skip_scan >> create_cumulative_manifest >> update_cumulative_manifest >> backup_manifest_task >> upload_new_manifest_task
