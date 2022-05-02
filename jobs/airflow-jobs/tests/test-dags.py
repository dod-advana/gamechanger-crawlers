import gzip as gz
import os
import tempfile
import unittest
from unittest import mock
from unittest.mock import Mock
from unittest.mock import MagicMock
from airflow.models import DAG, DagModel, DagRun, TaskInstance
from airflow.utils.types import DagRunType
from airflow.exceptions import AirflowException

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import ApiClient, models as k8s

import datetime

import boto3
import pytest
from botocore.exceptions import ClientError, NoCredentialsError

from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.s3 import S3Hook, provide_bucket_name, unify_bucket_name_and_key
from airflow.utils import timezone


# try to import the decorator mock_s3
try:
    from moto import mock_s3
except ImportError:
    mock_s3 = None

pytest_plugins = ["helpers_namespace"]


# If a task needs a dag to provide context


@pytest.fixture
def test_dag():
    """Airflow DAG for testing."""
    return DAG(
        "test_dag",
        start_date=datetime.datetime(2020, 1, 1),
        schedule_interval=datetime.timedelta(days=1),
        default_args={})


@pytest.helpers.register
def run_task(task, dag):
    """Run an Airflow task."""
    dag.clear()
    task.run(start_date=dag.start_date, end_date=dag.start_date)


DEFAULT_DATE = timezone.datetime(2016, 1, 1, 1, 0, 0)


def create_context(task, persist_to_db=False):
    dag = task.dag if task.has_dag() else DAG(dag_id="dag")
    dag_run = DagRun(
        run_id=DagRun.generate_run_id(DagRunType.MANUAL, DEFAULT_DATE),
        run_type=DagRunType.MANUAL,
        dag_id=dag.dag_id,
    )
    task_instance = TaskInstance(task=task, run_id=dag_run.run_id)
    task_instance.dag_run = dag_run
    if persist_to_db:
        with create_session() as session:
            session.add(DagModel(dag_id=dag.dag_id))
            session.add(dag_run)
            session.add(task_instance)
            session.commit()
    return {
        "dag": dag,
        "ts": DEFAULT_DATE.isoformat(),
        "task": task,
        "ti": task_instance,
        "task_instance": task_instance,
        "run_id": "test",
    }


def test_download_manifest(tmpdir):

    @mock_s3
    def download_manifest():
        bucket = "TESTBUCKET"
        key = "TESTKEY"
        source_s3 = S3Hook()

        # First need to set up the moto 'virtual' AWS account with bucket
        source_s3.create_bucket(bucket_name=bucket)
        # Path to write to
        tmpfile = tmpdir.join(key)
        # Create the tmpfile by writing 'hello' to it
        write_to_file = BashOperator(
            task_id="test", bash_command=f"echo 'hello' > {tmpfile} ")
        write_to_file.execute(context={})

        # Check that we have the tmpfile in the tmpdir
        assert len(tmpdir.listdir()) == 1

        # Upload tmpfile to mock_s3
        source_s3.load_file(
            filename=tmpfile.dirname + "/" + tmpfile.basename, key=key, bucket_name=bucket)

        # Main lines to test: Download tmpfile from mock_s3
        obj = source_s3.get_key(key, bucket)
        obj.download_file(str(tmpdir) + "/" + "test")

        # Files in tmpdir should be 2 now
        assert len(tmpdir.listdir()) == 2

    # Run download manifest task
    download_manifest_task = PythonOperator(
        task_id='copy_from_s3',
        python_callable=download_manifest
    )

    download_manifest_task.execute(context={})


def test_backup_manifest(tmpdir):
    @mock_s3
    def backup_manifest(**context):
        bucket = "TESTBUCKET"
        key = "TESTKEY"
        source_s3 = S3Hook()

        # First need to set up the moto 'virtual' AWS account with bucket
        source_s3.create_bucket(bucket_name=bucket)
        # Path to write to
        tmpfile = tmpdir.join(key)
        # Create the tmpfile by writing 'hello' to it
        write_to_file = BashOperator(
            task_id="test", bash_command=f"echo 'hello' > {tmpfile} ")
        write_to_file.execute(context={})

        # Upload tmpfile to mock_s3
        source_s3.load_file(
            filename=tmpfile.dirname + "/" + tmpfile.basename, key=key, bucket_name=bucket)

        # Lines to test: Backup tmpfile from mock_s3
        source_s3 = S3Hook()
        source_s3.copy_object(source_bucket_key=key, dest_bucket_key=key.split(".json")[0] + str(context['ts_nodash']),
                              source_bucket_name=bucket, dest_bucket_name=bucket)

        # Check if we now have 2 files in s3
        result = source_s3.list_keys(bucket_name=bucket)
        assert len(result) == 2

        # Check if the copy or one of the files has the appended timestamp
        assert any(str(context["ts_nodash"]) in s for s in result) == True

    # Run download manifest task
    backup_manifest_task = PythonOperator(
        task_id='copy_from_s3',
        python_callable=backup_manifest
    )

    backup_manifest_task.execute(context={"ts_nodash": "test20220427_"})


def test_purge_bash_operator(tmpdir):
    """Test purge bashoperator task's deletion of objects."""
    tmpfile = tmpdir.join("hello.txt")

    write_to_file = BashOperator(
        task_id="test", bash_command=f"echo 'hello' > {tmpfile} ")
    write_to_file.execute(context={})
    assert len(tmpdir.listdir()) == 1
    task = BashOperator(
        task_id="purge", bash_command=f"find {tmpdir} -name '*hello.txt' -type f -print0 | xargs -0 rm -f")
    result = task.execute(context={})
    assert len(tmpdir.listdir()) == 0


POD_MANAGER_CLASS = "airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager"


class TestKubernetesPodOperator:
    @pytest.fixture(autouse=True)
    def setup(self, dag_maker):
        self.create_pod_patch = mock.patch(f"{POD_MANAGER_CLASS}.create_pod")
        self.await_pod_patch = mock.patch(f"{POD_MANAGER_CLASS}.await_pod_start")
        self.await_pod_completion_patch = mock.patch(
            f"{POD_MANAGER_CLASS}.await_pod_completion")
        self.client_patch = mock.patch("airflow.kubernetes.kube_client.get_kube_client")
        self.create_mock = self.create_pod_patch.start()
        self.await_start_mock = self.await_pod_patch.start()
        self.await_pod_mock = self.await_pod_completion_patch.start()
        self.client_mock = self.client_patch.start()
        self.dag_maker = dag_maker

        yield

        self.create_pod_patch.stop()
        self.await_pod_patch.stop()
        self.await_pod_completion_patch.stop()
        self.client_patch.stop()

    def run_pod(self, operator: KubernetesPodOperator, map_index: int = -1) -> k8s.V1Pod:
        # need to randomize/change the dag id if run into unique error for sql alchemy
        with self.dag_maker(dag_id='dagtest12') as dag:
            operator.dag = dag

        dr = self.dag_maker.create_dagrun(run_id='test')
        (ti,) = dr.task_instances
        ti.map_index = map_index
        self.dag_run = dr
        context = ti.get_template_context(session=self.dag_maker.session)
        self.dag_maker.session.commit()  # So 'execute' can read dr and ti.

        remote_pod_mock = MagicMock()
        remote_pod_mock.status.phase = 'Succeeded'
        self.await_pod_mock.return_value = remote_pod_mock
        operator.execute(context=context)
        return self.await_start_mock.call_args[1]['pod']

    def test_purge_pod(self):
        file_path = "tmp/fake_file"

        purge = KubernetesPodOperator(namespace="default",
                                      image="busybox",
                                      name="test",
                                      task_id="task",
                                      cmds=['/bin/sh', '-c',
                                            "find /app/tmp/downloads/ -name '*' -type f -print0 | xargs -0 rm -f"],
                                      in_cluster=False,
                                      do_xcom_push=False,
                                      #   volumes=[downloads_volume],
                                      #   volume_mounts=[downloads_volume_mount],
                                      config_file=file_path,
                                      cluster_context="default",
                                      )
        remote_pod_mock = MagicMock()
        remote_pod_mock.status.phase = 'Succeeded'
        self.await_pod_mock.return_value = remote_pod_mock
        self.client_mock.list_namespaced_pod.return_value = []
        self.run_pod(purge)
        self.client_mock.assert_called_once_with(
            in_cluster=False,
            cluster_context="default",
            config_file=file_path
        )

    def test_crawl_pod(
        self,
    ):

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

        env_from = [aws_env_from_source,
                    download_env_from_source, uploader_env_from_source]

        crawl = KubernetesPodOperator(namespace="airflow",
                                      image="092912502985.dkr.ecr.us-east-1.amazonaws.com/gc-crawler-downloader-test:latest",
                                      name="crawler-test",
                                      env_from=env_from,
                                      task_id="crawler-task",
                                      security_context={'runAsUser': 1000,
                                                        'runAsGroup': 1000,
                                                        'fsGroup': 1000},
                                      get_logs=True,
                                      is_delete_operator_pod=True,
                                      arguments=["crawl"],
                                      )

        pod = self.run_pod(crawl)
        assert pod.spec.containers[0].env_from == env_from
        assert pod.spec.security_context["fsGroup"] == 1000

    def test_scan_pod(
        self,
    ):
        aws_role = {
            'iam.amazonaws.com/role': 'advana/k8s/s3.wildcard'},
        scanner_configmap_source = k8s.V1ConfigMapEnvSource(
            name='scanner-uploader-configmap')
        scanner_env_from_source = k8s.V1EnvFromSource(
            config_map_ref=scanner_configmap_source)

        scan_upload = KubernetesPodOperator(namespace="airflow",
                                            image="092912502985.dkr.ecr.us-east-1.amazonaws.com/gc-crawler-downloader-test:latest",
                                            name="scanupload-test",
                                            env_from=[scanner_env_from_source],
                                            task_id="scanupload-task",
                                            get_logs=True,
                                            is_delete_operator_pod=True,
                                            arguments=['scan'],
                                            annotations=aws_role)
        pod = self.run_pod(scan_upload)
        assert pod.spec.containers[0].env_from == [scanner_env_from_source]
        assert pod.metadata.annotations == aws_role
