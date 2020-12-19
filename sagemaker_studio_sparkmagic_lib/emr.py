import json
import os
import sys

import boto3
import botocore
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))


class EMRCluster:
    """
    class that represents emr cluster and provides helper functions to get relevant attributes
    needed to generate configurations
    """

    def __init__(self, cluster_id=None, role_arn=None, emr=None):
        if not emr:
            sess = self._get_boto3_session(role_arn)
            emr = sess.client("emr")
        self._get_cluster(emr, cluster_id)
        self._get_instances(emr, cluster_id)
        self._get_security_conf(emr)
        logger.info(f"Successfully read emr cluster({cluster_id}) details")

    def _get_boto3_session(self, role_arn):
        """
        Allows cross account cluster access
        """
        if not role_arn:
            return boto3.session.Session()
        else:
            logger.info(f"Assuming role: {role_arn}")
            sts_client = boto3.client("sts")
            try:
                assume_role_object = sts_client.assume_role(
                    RoleArn=role_arn, RoleSessionName="SageMakerStudioUser"
                )
            except botocore.exceptions.ClientError as ce:
                logger.debug(
                    f"Failed to assume role: ({role_arn}) details. {ce.response}"
                )
                raise ValueError(
                    f"Unable to assume role(arn: {role_arn}). Ensure permissions are setup correctly"
                    f' Error: {ce.response["Error"]}'
                ) from None
            return boto3.Session(
                aws_access_key_id=assume_role_object["Credentials"]["AccessKeyId"],
                aws_secret_access_key=assume_role_object["Credentials"][
                    "SecretAccessKey"
                ],
                aws_session_token=assume_role_object["Credentials"]["SessionToken"],
            )

    def _get_cluster(self, emr, cluster_id):
        try:
            describe_cluster_response = emr.describe_cluster(ClusterId=cluster_id)
        except botocore.exceptions.ClientError as ce:
            logger.debug(
                f"Failed to get EMR cluster({cluster_id}) details. {ce.response}"
            )
            raise ValueError(
                f"Unable to get Emr Cluster(Id: {cluster_id}) details using describe-cluster API."
                f' Error: {ce.response["Error"]}'
            ) from None
        logger.debug(f"Describe emr cluster response: {describe_cluster_response}")
        self._cluster = describe_cluster_response["Cluster"]

    def _get_instances(self, emr, cluster_id):
        try:
            list_instances_response = emr.list_instances(ClusterId=cluster_id)
        except botocore.exceptions.ClientError as ce:
            logger.debug(
                f"Failed to list instances in  EMR cluster({cluster_id}) details. {ce.response}"
            )
            raise ValueError(
                f"Unable to list instances in EMR Cluster(Id: {cluster_id}) details using list-instances API."
                f' Error: {ce.response["Error"]}'
            ) from None
        logger.debug(f"List instances response: {list_instances_response}")
        self._instances = list_instances_response["Instances"]

    def _get_security_conf(self, emr):
        if "SecurityConfiguration" not in self._cluster:
            logger.debug(
                "Skipping describing security group as no specific security configuration is used for cluster"
            )
            self._sec_conf = None
            return
        security_conf = self._cluster["SecurityConfiguration"]
        cluster_id = self._cluster["Id"]
        try:
            describe_sec_conf_response = emr.describe_security_configuration(
                Name=security_conf
            )
        except botocore.exceptions.ClientError as ce:
            logger.debug(
                f"Failed to get security configuration details({security_conf}) of EMR cluster({cluster_id})"
                f"details. {ce.response}"
            )
            raise ValueError(
                f"Unable to get security configuration details({security_conf}) of EMR CLuster(Id: {cluster_id})"
                f'details using describe-security-configuration. Error: {ce.response["Error"]}'
            ) from None
        logger.debug(
            f"Describe emr security config response: {describe_sec_conf_response}"
        )
        self._sec_conf = json.loads(describe_sec_conf_response["SecurityConfiguration"])

    @property
    def is_krb_cluster(self):
        """
        Returns true if EMR cluster is configured with kerberos
        """
        return not (
            "KerberosAttributes" not in self._cluster
            or len(self._cluster["KerberosAttributes"]) == 0
        )

    def primary_node_private_dns_name(self):
        """
        Returns primary node private DNS name using describe cluster result
        """
        # Master node public dns can actually be private DNS if EMR cluster is launched in
        # private subnet hence we check both names. See
        # https://docs.aws.amazon.com/emr/latest/APIReference/API_Cluster.html
        master_node_public_dns = self._cluster["MasterPublicDnsName"]
        for instance in self._instances:
            if (
                "PublicDnsName" in instance
                and instance["PublicDnsName"] == master_node_public_dns
            ) or (
                "PrivateDnsName" in instance
                and instance["PrivateDnsName"] == master_node_public_dns
            ):
                master_instance_details = instance

        if master_instance_details is None:
            raise ValueError(
                f"Failed to find primary node ip address needed to communicate with Livy server."
                f"Please ensure clusterId{self._cluster['Id']} is correct"
            ) from None

        return master_instance_details["PrivateDnsName"]

    def get_krb_conf(self):
        """
        Generate kerberos configuration parameters for a given EMR cluster.
        These configuration parameters are used for constructing krb5.conf
        Returned configuration is a two layered dictionary with top level keys for "sections"
        defined in kerberos config
        https://web.mit.edu/kerberos/krb5-1.12/doc/admin/conf_files/krb5_conf.html#sections
        {
            "libdefaults": {}
            "realms": {}
            "domain_realm": {}
        }
        """
        # Good overview of kerberos configuration
        # https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-create-security-configuration.html#emr-kerberos-cli-parameters
        # Kerberos server config on EMR cluster
        properties = {}
        emr_realm_name = self._cluster["KerberosAttributes"]["Realm"]
        emr_realm_attr = {
            "kdc": f"{self.primary_node_private_dns_name()}:88",
            "admin_server": f"{self.primary_node_private_dns_name()}:749",
            "default_domain": f"{self._get_region()}.compute.internal",
        }

        properties["realms"] = {emr_realm_name: emr_realm_attr}
        properties["libdefaults"] = {"default_realm": emr_realm_name}
        properties["domain_realm"] = {
            f"{self._get_region()}.compute.internal": f"{emr_realm_name}",
            # not the same key as above. There is a dot at the beginning of key
            f".{self._get_region()}.compute.internal": f"{emr_realm_name}",
        }
        sec_krb_conf = self._sec_conf["AuthenticationConfiguration"][
            "KerberosConfiguration"
        ]
        krb_provider = sec_krb_conf["Provider"]
        if krb_provider == "ClusterDedicatedKdc":
            kdc_conf = sec_krb_conf["ClusterDedicatedKdcConfiguration"]
            properties["libdefaults"][
                "ticket_lifetime"
            ] = f'{kdc_conf["TicketLifetimeInHours"]}h'
            if "CrossRealmTrustConfiguration" in kdc_conf:
                cross_real_conf = kdc_conf["CrossRealmTrustConfiguration"]
                cross_realm = cross_real_conf["Realm"]
                cross_domain = cross_real_conf["Domain"]
                properties["realms"][cross_real_conf["Realm"]] = {
                    "kdc": cross_real_conf["KdcServer"],
                    "admin_server": cross_real_conf["AdminServer"],
                    "default_domain": cross_real_conf["Domain"],
                }
                properties["domain_realm"][cross_domain] = cross_realm
                properties["domain_realm"][f".{cross_domain}"] = cross_realm
        elif krb_provider == "ExternalKdc":
            kdc_conf = sec_krb_conf["ExternalKdcConfiguration"]
            properties["libdefaults"][
                "ticket_lifetime"
            ] = f'{kdc_conf["TicketLifetimeInHours"]}h'
            ad_integ_conf = kdc_conf["AdIntegrationConfiguration"]
            properties["realms"][ad_integ_conf["AdRealm"]] = {
                "kdc": kdc_conf["KdcServer"],
                "admin_server": kdc_conf["AdminServer"],
                "default_domain": ad_integ_conf["AdDomain"],
            }
            ext_realm = ad_integ_conf["AdRealm"]
            ext_domain = ad_integ_conf["AdDomain"]
            properties["domain_realm"][ext_domain] = ext_realm
            properties["domain_realm"][f".{ext_domain}"] = ext_realm

        return properties

    def _get_region(self):
        return os.getenv("AWS_REGION", "us-west-2")
