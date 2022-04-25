from datetime import datetime
# from fileinput import filename
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from kubernetes.client import models as k8s

# Airflow dag metadata
args = {
    "owner": "gamechanger",
    "depends_on_past": False,
    "retries": 1,
    "catchup_by_default": False,
}

# 3 sections below to create volumes to pass to pods for crawler results
# mounts
results_volume_mount = VolumeMount('gc-crawler-output',
                                   mount_path='/app/tmp/output/',
                                   sub_path=None,
                                   read_only=False
                                   )
schedules_volume_mount = VolumeMount('crawler-schedule-volume',
                                     mount_path='/app/tmp/crawler-schedules/',
                                     sub_path=None,
                                     read_only=False
                                     )
# volume configs
results_volume_config = {
    'persistentVolumeClaim':
    {
        'claimName': 'gc-crawler-output'
    }
}
schedule_volume_config = {
    'configMap':
    {
        'name': 'crawler-schedules-configmap'
    }
}
# volume objects to attach to pod
results_volume = Volume(name='gc-crawler-output',
                        configs=results_volume_config)
schedule_volume = Volume(name='crawler-schedule-volume',
                         configs=schedule_volume_config)

# volume objects to download previous manifest from S3, uses the k8s model since the previous airflow type is deprecated for use with executor_config's pod_override
downloads_volume_mount = k8s.V1VolumeMount(name='gc-crawler-downloads', mount_path="/app/tmp/downloads/",
                                           sub_path=None,
                                           read_only=False)
pvc = k8s.V1PersistentVolumeClaimVolumeSource(
    claim_name='gc-crawler-downloads')
downloads_volume = k8s.V1Volume(
    name='gc-crawler-downloads', persistent_volume_claim=pvc)

# list of configmaps for tasks that use kubernetespodoperator
crawler_configmaps = ["crawler-configmap"]
scanner_configmaps = ["scanner-uploader-configmap"]

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
    source_s3.copy_object(source_bucket_key=key, dest_bucket_key=key.split(".json")[0] + str(context['ts_nodash']),
                          source_bucket_name=bucket, dest_bucket_name=bucket)


def upload_manifest():
    import os
    AWS_S3_CONN_ID = os.environ["AWS_S3_CONN_ID"]
    bucket = os.environ["BUCKET"]
    key = os.environ["KEY"]
    filename = os.environ["FILENAME"]
    source_s3 = S3Hook(AWS_S3_CONN_ID)
    source_s3.load_file(filename=filename, key=key,
                        bucket_name=bucket, replace=True)


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
                              name="purge-dl-volume",
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

# Executor starts a pod that runs the crawler. Configs from the Gamechanger helm deployments from
crawl = KubernetesPodOperator(namespace="airflow",
                              image="092912502985.dkr.ecr.us-east-1.amazonaws.com/gc-crawler-downloader-test:latest",
                              name="crawler-test",
                              configmaps=crawler_configmaps,
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

# scan downloaded crawled files then upload to s3
scan_upload = KubernetesPodOperator(namespace="airflow",
                                    image="092912502985.dkr.ecr.us-east-1.amazonaws.com/gc-crawler-downloader-test:latest",
                                    name="scanupload-test",
                                    configmaps=scanner_configmaps,
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
                                                   configmaps=[
                                                       "crawler-configmap"],
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
                                                   security_context={'runAsUser': 1000,
                                                                     'runAsGroup': 1000,
                                                                     'fsGroup': 1000},
                                                   volumes=[
                                                       downloads_volume],
                                                   volume_mounts=[
                                                       downloads_volume_mount],
                                                   configmaps=[
                                                       "crawler-configmap"],
                                                   dag=dag
                                                   )

upload_new_manifest_task = PythonOperator(task_id='upload_new_s3_manifest',
                                          python_callable=upload_manifest,
                                          dag=dag,
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


purge.set_downstream(download_manifest_task)
download_manifest_task.set_downstream(crawl)
crawl.set_downstream(scan_upload)
scan_upload.set_downstream(create_cumulative_manifest)
create_cumulative_manifest.set_downstream(update_cumulative_manifest)
update_cumulative_manifest.set_downstream(backup_manifest_task)
backup_manifest_task.set_downstream(upload_new_manifest_task)
