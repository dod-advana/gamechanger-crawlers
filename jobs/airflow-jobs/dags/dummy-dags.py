from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
# from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount
from kubernetes.client import models as k8s
from airflow.models import Connection
from airflow.models import Variable
import os


def print_env():
    for k, v in sorted(os.environ.items()):
        print(k + ':', v)
    print('\n')


# Airflow UI metadata
args = {
    "owner": "gamechanger",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "catchup_by_default": False,
}


# DAG definition
with DAG(
    dag_id="tester-dag",
    description="for testing",
    default_args=args,
    start_date=datetime(2022, 3, 8, 14, 30),
    catchup=False,
    max_active_runs=1,
    tags=["test"],
) as dag:
    #     grab_manifest = KubernetesPodOperator(namespace="jupyterhub",
    #                                           image="registry.lab.boozallencsn.com/ironbank-mirror/ironbank/opensource/amazon/aws-cli:2.4.25",
    #                                           name="ls-bucket",
    #                                           task_id="ls-bucket-task",
    # #                                           configmaps=['certs-configmap'],
    #                                           volumes=[cert_volume],
    #                                           volume_mounts=[cert_volume_mount],
    #                                           init_containers=[init_container],
    #                                           cmds=["echo 'hello world'"],
    # #                                           cmds=["aws configure set aws_access_key_id $key_id  && aws configure set aws_secret_access_key && aws s3 ls --endpoint-url 'https://s3.lab.boozallencsn.com' --no-ssl-verify"],
    #                                           is_delete_operator_pod=True,
    #                                           dag=dag
    #                                          )
    #     hello_world = KubernetesPodOperator(namespace="jupyterhub",
    #                                           image="registry.lab.boozallencsn.com/docker-mirror/library/busybox",
    #                                           name="hello-world",
    #                                           task_id="hello-task",
    #                                           init_containers=[init_container],
    #                                           cmds=["echo 'hello world'"],
    #                                        )

    # scan_upload = KubernetesPodOperator(namespace="jupyterhub",
    #                                 image=scanner_image,
    #                                 name="scanupload-task",
    #                                 task_id="scanupload-task",
    #                                 get_logs=True,
    #                                 env_vars={"AWS_CA_BUNDLE": credentials_dict["aws_ca_bundle_configmap_path"], "AWS_ACCESS_KEY_ID" : credentials_dict["aws_access_key_id"], "AWS_SECRET_ACCESS_KEY" : credentials_dict["aws_secret_access_key"]},
    #                                 is_delete_operator_pod=True,
    #                                 cmds=["/bin/sh", "-c"],
    #                                 arguments=["echo $AWS_CA_BUNDLE && aws --endpoint-url "+credentials_dict['host']+" s3 cp s3://gamechanger/bronze/gamechanger/data-pipelines/orchestration/crawlers /tmp --recursive && ls /tmp"],
    #                                 volumes=[
    #                                     cert_volume],
    #                                 volume_mounts=[
    #                                     cert_volume_mount],
    #                                 dag=dag)

    print_env_var = PythonOperator(python_callable=print_env,
                                   task_id="print-env-var")
