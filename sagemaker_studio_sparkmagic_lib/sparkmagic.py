import json
import os
import sys
import IPython
import logging

from sagemaker_studio_sparkmagic_lib.emr import EMRCluster
from sagemaker_studio_sparkmagic_lib.kerberos import write_krb_conf

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))

# We are not storing SparkMagic conf files on persistent volume
# to allow different SparkMagic configuration across apps
SPARKMAGIC_CONF_DIR = "/etc/sparkmagic"
SPARKMAGIC_CONF_FILE = "config.json"


def connect_to_emr_cluster(
    cluster_id,
    user_name="livy",
    role_arn=None,
    restart_kernel=False,
    skip_krb=False,
    # override_path for local testing
    krb_file_override_path=None,
    spark_magic_override_path=None,
):
    """
    Sets up configuration file needed to connect to EMR cluster.
    1) Sets SparkMagic configuration file so it can connect to livy server on emr cluster. Restarts kernel to pick
    up new values.
    2) If EMR cluster uses kerberos to authentication, it also generates krb.conf file for that cluster.
    :param str cluster_id: EMR cluster Id
    :param str user_name: user_name that is used when submitting jobs to livy.
    :param str role_arn: role_arn to assume to get cluster details. Allows cross account cluster access setup
    :param boolean restart_kernel: one can use this option to disable kernel restart, used when calling from cli
    :param str krb_file_override_path: kerberos override file path
    :param spark_magic_override_path: sparkmagic config file path
    """
    cluster = EMRCluster(cluster_id=cluster_id, role_arn=role_arn)
    # array to store user steps as we setup each config
    user_steps = []

    # 1. setup spark magic conf
    _write_spark_magic_conf(cluster, user_name, skip_krb, spark_magic_override_path)
    logger.info(
        f"SparkMagic config file location: {SPARKMAGIC_CONF_DIR}/{SPARKMAGIC_CONF_FILE}"
    )

    # 2. setup kerberos
    if _is_krb_cluster(cluster, skip_krb):
        # standard kerberos file location
        krb_file_dir = f"/etc"
        krb_file_path = os.path.join(krb_file_dir, "krb5.conf")
        if krb_file_override_path:
            # for local testing
            logger.info(
                f"Using override path for kerberos file: {krb_file_override_path}"
            )
            krb_file_path = krb_file_override_path
        os.makedirs(krb_file_dir, exist_ok=True)
        write_krb_conf(cluster, krb_file_path)
        logger.info(f"Kerberos configuration file location: {krb_file_path}")

    if cluster.is_krb_cluster:
        krb_user_name = user_name
        if user_name == "livy":
            # user has not provided a user name as input, we use place holder to hint user to use actual user name
            krb_user_name = "$user"
        user_steps.append(
            f"Open the image terminal and run 'kinit {cluster.get_kinit_user_name(krb_user_name)}' to get kerberos ticket"
        )

    # auto-restart kernel
    # This is disabled from cli as it does not work
    if restart_kernel:
        logger.info(
            "Restarting kernel. To manually restart (set restart_kernel to False)"
        )
        _restart_kernel()
    else:
        user_steps.append(
            "Restart the kernel. This is required so that SparkMagic can pickup the generated configuration"
        )

    logger.info(
        f"Completed setting up configuration files for SparkMagic to connect to EMR cluster {cluster_id}"
    )

    if len(user_steps) > 0:
        logger.info("\n")
        next_steps = "To complete the setup, follow these steps:\n"
        for i in range(len(user_steps)):
            next_steps += f"{i + 1}. {user_steps[i]}\n"

        # printing colored text so user can ignore previous logs and next steps are clear
        # ref: https://stackoverflow.com/questions/287871/how-to-print-colored-text-in-python
        logger.info(f"\033[93m{next_steps}")


def _is_krb_cluster(cluster, skip_krb):
    return cluster.is_krb_cluster and not skip_krb


def _write_spark_magic_conf(cluster, user_name, skip_krb, spark_magic_override_path):
    """
    example config: https://github.com/jupyter-incubator/sparkmagic/blob/master/sparkmagic/example_config.json
    """
    here = os.path.dirname(__file__)
    with open(os.path.join(here, "data", "sample_config.json")) as f:
        basic_conf = json.load(f)

    basic_conf["kernel_python_credentials"] = {
        "username": user_name,
        "password": "",
        "url": f"http://{cluster.primary_node_private_dns_name()}:8998",
        "auth": "None",
    }
    basic_conf["ignore_ssl_errors"] = True

    if _is_krb_cluster(cluster, skip_krb):
        basic_conf["kernel_python_credentials"]["auth"] = "Kerberos"
        # kerberos default values copied from example config
        basic_conf["kerberos_auth_configuration"] = {
            "mutual_authentication": 1,
            "service": "HTTP",
            "delegate": False,
            "force_preemptive": True,
            "principal": "",
            "hostname_override": cluster.krb_hostname_override(),
            "sanitize_mutual_error_response": True,
            "send_cbt": False,
        }

    file_path = os.path.join(SPARKMAGIC_CONF_DIR, SPARKMAGIC_CONF_FILE)
    if spark_magic_override_path:
        logger.info(
            f"Using override path for SparkMagic config file: {spark_magic_override_path}"
        )
        file_path = spark_magic_override_path
    else:
        os.makedirs(SPARKMAGIC_CONF_DIR, exist_ok=True)
    with open(file_path, "w") as f:
        json.dump(basic_conf, f, indent=2)


def _restart_kernel():
    IPython.Application.instance().kernel.do_shutdown(restart=True)
