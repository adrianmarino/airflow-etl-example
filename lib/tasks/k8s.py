import os
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator


class K8sTasks:
    @classmethod
    def create(
        cls,
        task_id,
        cmds                    = [],
        image                   = 'condaforge/mambaforge',
        labels                  = {'source': 'airflow'},
        namespace               = Variable.get('k8s_namespace'),
        startup_timeout_seconds = 1000,
        image_pull_policy       = 'IfNotPresent',
        resources               = {
            'limit_memory': "2048M",
            'limit_cpu': "500m"
        },
        do_xcom_push            = True
    ):
        """Create a KubernetesPodOperator definci

        Args:
            task_id (string): task identifier.

            cmds (list, optional):  Entrypoint of the container, if not specified the Docker container's
                                    entrypoint is used. The cmds parameter is templated. Defaults to [].

            image (str, optional): A docker image. Defaults to 'condaforge/mambaforge'.

            labels (dict, optional): Labels to apply to the Pod. Defaults to {'source': 'airflow'}.

            namespace (str, optional): Kubernate namespace. Defaults to Variable.get('k8s_namespace').

            startup_timeout_seconds (int, optional): If true, logs stdout output of container. Defaults to True. Defaults to 1000.

            image_pull_policy (str, optional):  Determines when to pull a fresh image, if 'IfNotPresent' will cause the
                                                Kubelet to skip pulling an image if it already exists. If you want to
                                                always pull a new image, set it to 'Always'. Defaults to 'IfNotPresent'.

            resources (dict, optional): Resource specifications for Pod, this will allow you to set both cpu and
                                        memory limits and requirements.Prior to Airflow 1.10.4, resource specifications
                                        were passed as a Pod Resources Class object, If using this example on a version
                                        of Airflow prior to 1.10.4, import the "pod" package from airflow.contrib.kubernetes
                                        and use resources = pod.Resources() instead passing a dict.
                                        For more info see: https://github.com/apache/airflow/pull/4551.
                                        Defaults to { 'limit_memory': "2048M", 'limit_cpu': "500m" }.

            do_xcom_push (bool, optional):  If true, the content of /airflow/xcom/return.json from container will also be
                                            pushed to an XCom when the container ends. Defaults to True.

        Returns:
            KubernetesPodOperator: a KubernetesPodOperator instance.
        """
        return KubernetesPodOperator(
            namespace               = namespace,
            image                   = image,
            cmds                    = cmds,
            labels                  = labels,
            name                    = task_id,
            task_id                 = task_id,
            get_logs                = True,
            is_delete_operator_pod  = True,
            in_cluster              = False,
            startup_timeout_seconds = startup_timeout_seconds,
            image_pull_policy       = image_pull_policy
        )
