import json
import os
import sys

import boto3
import botocore
import logging
from sagemaker_studio_sparkmagic_lib import utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))


class EMRCluster:
    """
    class that represents emr cluster and provides helper functions to get relevant attributes
    needed to generate configurations
    """

    def __init__(self, cluster_id=None, role_arn=None, emr=None):
        self._cluster = None
        self._instances = None
        self._sec_conf = None
        self._emr_primary_instance = None

        if not emr:
            sess = self._get_boto3_session(role_arn)
            emr = sess.client(
                "emr", endpoint_url=utils.get_emr_endpoint_url(self._get_region())
            )
        self._get_cluster(emr, cluster_id)
        self._get_instances(emr, cluster_id)
        self._get_security_conf(emr)
        logger.info(f"Successfully read emr cluster({cluster_id}) details")
        self._identify_emr_primary_instance()

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
            paginator = emr.get_paginator("list_instances")
            page_iterator = paginator.paginate(ClusterId=cluster_id)
            instances = []
            for page in page_iterator:
                instances.extend(page["Instances"])
        except botocore.exceptions.ClientError as ce:
            logger.debug(
                f"Failed to list instances in  EMR cluster({cluster_id}) details. {ce.response}"
            )
            raise ValueError(
                f"Unable to list instances in EMR Cluster(Id: {cluster_id}) details using list-instances API."
                f' Error: {ce.response["Error"]}'
            ) from None
        logger.debug(f"List instances response: {instances}")
        self._instances = instances

    def _identify_emr_primary_instance(self):
        """
        Identifies EMR primary node.
        """
        # Primary node public dns can actually be private DNS if EMR cluster is launched in
        # private subnet hence we check both names. See
        # https://docs.aws.amazon.com/emr/latest/APIReference/API_Cluster.html
        primary_node_public_dns = self._cluster["MasterPublicDnsName"]
        for instance in self._instances:
            if (
                "PublicDnsName" in instance
                and instance["PublicDnsName"] == primary_node_public_dns
            ) or (
                "PrivateDnsName" in instance
                and instance["PrivateDnsName"] == primary_node_public_dns
            ):
                self._emr_primary_instance = instance

        if self._emr_primary_instance is None:
            raise ValueError(
                f"Failed to find primary node ip address needed to communicate with Livy server."
                f"Please ensure clusterId{self._cluster['Id']} is correct"
            ) from None

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
        Returns primary node private DNS name
        """
        return self._emr_primary_instance["PrivateDnsName"]

    def primary_node_public_dns_name(self):
        """
        Returns primary node public DNS name
        """
        return self._emr_primary_instance["PublicDnsName"]

    def krb_hostname_override(self):
        """
        According to kerberos https://github.com/requests/requests-kerberos/blob/master/README.rst
        "
        If communicating with a host whose DNS name doesn't match its kerberos hostname (eg, behind a content switch or
        load balancer), the hostname used for the Kerberos GSS exchange can be overridden by setting the hostname_override
        "

        Since we are communicating from studio host not registered in EMR one has to override hostname matching DNS/DHCP options
        """
        hostname = self.primary_node_private_dns_name()
        search = utils.get_domain_search(self._get_region())
        if search != utils.get_default_domain_search(self._get_region()):
            names = hostname.split(".")
            if len(names) > 1:
                hostname = f"{names[0]}.{search}"

        return hostname

    def get_kinit_user_name(self, user_name):
        """
        Generates exact user name to be used for kinit. Depending on Kerberos configuration user may
        have to use realm name to do kinit
        """
        if self.is_krb_cluster:
            sec_krb_conf = self._sec_conf["AuthenticationConfiguration"][
                "KerberosConfiguration"
            ]
            krb_provider = sec_krb_conf["Provider"]
            if krb_provider == "ExternalKdc":
                kdc_conf = sec_krb_conf["ExternalKdcConfiguration"]
                ad_integ_conf = kdc_conf["AdIntegrationConfiguration"]
                return f"{user_name}@{ad_integ_conf['AdRealm']}"

        return user_name

    def is_external_kdc(self):
        if not self.is_krb_cluster:
            return False
        sec_krb_conf = self._sec_conf["AuthenticationConfiguration"][
            "KerberosConfiguration"
        ]
        krb_provider = sec_krb_conf["Provider"]

        return krb_provider == "ExternalKdc"

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
        # DNS Search value is used for domain_realm mappings and other kerberos properties
        # We have learned that using DNS Search is needed instead of AWS default DNS values for kerberos to work
        search = utils.get_domain_search(self._get_region())

        emr_realm_attr = {
            "kdc": f"{self.primary_node_private_dns_name()}:88",
            "admin_server": f"{self.primary_node_private_dns_name()}:749",
            "default_domain": f"{search}",
        }

        properties["realms"] = {emr_realm_name: emr_realm_attr}
        properties["libdefaults"] = {"default_realm": emr_realm_name}

        # map emr realm to domain search value
        properties["domain_realm"] = {
            search: f"{emr_realm_name}",
            # not the same key as above. There is a dot at the beginning of key
            f".{search}": f"{emr_realm_name}",
        }
        sec_krb_conf = self._sec_conf["AuthenticationConfiguration"][
            "KerberosConfiguration"
        ]
        krb_provider = sec_krb_conf["Provider"]
        if krb_provider == "ClusterDedicatedKdc":
            kdc_conf = sec_krb_conf["ClusterDedicatedKdcConfiguration"]
            properties["libdefaults"][
                "ticket_lifetime"
            ] = f'{kdc_conf.get("TicketLifetimeInHours", "24")}h'
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
            ] = f'{kdc_conf.get("TicketLifetimeInHours", "24")}h'

            # For external kdc configuration default realm-properties should point to external kdc server
            emr_realm_attr = {
                "kdc": kdc_conf["KdcServer"],
                "admin_server": kdc_conf["AdminServer"],
                "default_domain": f"{search}",
            }

            properties["realms"][emr_realm_name] = emr_realm_attr
            ad_integ_conf = kdc_conf["AdIntegrationConfiguration"]
            properties["realms"][ad_integ_conf["AdRealm"]] = {
                # For external kdc configuration parameter AdServer is not documented in EMR public docs
                # We found from some use cases that parameter AdServer Exists
                # and it should be used as KDC Server
                "kdc": ad_integ_conf.get("AdServer", ad_integ_conf["AdDomain"]),
                "admin_server": ad_integ_conf.get(
                    "AdServer", ad_integ_conf["AdDomain"]
                ),
                "default_domain": ad_integ_conf["AdDomain"],
            }
            ext_realm = ad_integ_conf["AdRealm"]
            ext_domain = ad_integ_conf["AdDomain"]
            properties["domain_realm"][ext_domain] = ext_realm
            properties["domain_realm"][f".{ext_domain}"] = ext_realm

        return properties

    def _get_region(self):
        return os.getenv("AWS_REGION", "us-west-2")
