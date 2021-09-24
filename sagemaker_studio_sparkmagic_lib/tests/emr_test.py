from unittest.mock import patch

import boto3
import pytest
from botocore.stub import Stubber

from sagemaker_studio_sparkmagic_lib.emr import EMRCluster


def test_get_cluster_happy_case_non_kerberos():
    emr = boto3.client("emr", region_name="us-west-2")
    with Stubber(emr) as emr_stub:
        describe_cluster_response = {
            "Cluster": {"Id": "j-3DD9ZR01DAU14", "Name": "Mycluster"}
        }
        list_instances_response = {
            "Instances": [
                {
                    "Id": "j-3DD9ZR01DAU14",
                }
            ]
        }
        emr_stub.add_response("describe_cluster", describe_cluster_response)
        emr_stub.add_response("list_instances", list_instances_response)
        emr_cluster = EMRCluster(cluster_id="j-3DD9ZR01DAU14", emr=emr)

        assert emr_cluster._cluster == describe_cluster_response["Cluster"]
        assert emr_cluster._instances == list_instances_response["Instances"]
        assert emr_cluster._sec_conf is None
        assert not emr_cluster.is_krb_cluster


def test_get_cluster_happy_case_non_kerberos_with_pagination():
    emr = boto3.client("emr", region_name="us-west-2")
    with Stubber(emr) as emr_stub:
        describe_cluster_response = {
            "Cluster": {"Id": "j-3DD9ZR01DAU14", "Name": "Mycluster"}
        }
        list_instances_response_page_1 = {
            "Instances": [
                {
                    "Id": "j-7DD9ZR01DAU99",
                }
            ],
        }
        list_instances_response_page_2 = {
            "Instances": [
                {
                    "Id": "j-3DD9ZR01DAU14",
                }
            ]
        }
        emr_stub.add_response("describe_cluster", describe_cluster_response)
        emr_stub.add_response("list_instances", list_instances_response_page_1)
        emr_stub.add_response("list_instances", list_instances_response_page_2)
        emr_cluster = EMRCluster(cluster_id="j-3DD9ZR01DAU14", emr=emr)

        assert emr_cluster._cluster == describe_cluster_response["Cluster"]
        assert (
            emr_cluster._instances
            == list_instances_response_page_1["Instances"]
            + list_instances_response_page_2["Instances"]
        )
        assert emr_cluster._sec_conf is None
        assert not emr_cluster.is_krb_cluster


def test_get_cluster_happy_case_kerberos():
    emr = boto3.client("emr", region_name="us-west-2")
    with Stubber(emr) as emr_stub:
        describe_cluster_response = {
            "Cluster": {
                "Id": "j-3DD9ZR01DAU14",
                "Name": "Mycluster",
                "SecurityConfiguration": "kerb-security-config",
            }
        }
        list_instances_response = {
            "Instances": [
                {
                    "Id": "j-3DD9ZR01DAU14",
                }
            ]
        }
        describe_sec_conf_response = {
            "Name": "kerb-security-config",
            "SecurityConfiguration": "{}",
        }
        emr_stub.add_response("describe_cluster", describe_cluster_response)
        emr_stub.add_response("list_instances", list_instances_response)
        emr_stub.add_response(
            "describe_security_configuration", describe_sec_conf_response
        )
        emr_cluster = EMRCluster(cluster_id="j-3DD9ZR01DAU14", emr=emr)

        assert emr_cluster._cluster == describe_cluster_response["Cluster"]
        assert emr_cluster._instances == list_instances_response["Instances"]
        assert emr_cluster._sec_conf == {}


def test_get_bad_cluster():
    emr = boto3.client("emr", region_name="us-west-2")
    with Stubber(emr) as emr_stub:
        list_instances_response = {
            "Instances": [
                {
                    "Id": "j-3DD9ZR01DAU14",
                }
            ]
        }
        emr_stub.add_client_error("describe_cluster", service_error_code=400)
        emr_stub.add_response("list_instances", list_instances_response)
        with pytest.raises(ValueError) as e:
            EMRCluster(cluster_id="j-3DD9ZR01DAU14", emr=emr)


@patch("sagemaker_studio_sparkmagic_lib.utils.get_domain_search")
def test_cluster_dedicated_krb_cluster(mock_utils):
    mock_utils.return_value = "test.me"
    emr = boto3.client("emr", region_name="us-west-2")
    with Stubber(emr) as emr_stub:
        describe_cluster_response = {
            "Cluster": {
                "Id": "j-3DD9ZR01DAU14",
                "Name": "Mycluster",
                "SecurityConfiguration": "kerb-security-config",
                "KerberosAttributes": {
                    "Realm": "KTEST.COM",
                    "KdcAdminPassword": "********",
                },
                "MasterPublicDnsName": "ec2-34-222-47-14.us-west-2.compute.amazonaws.com",
            }
        }
        list_instances_response = {
            "Instances": [
                {
                    "Id": "j-3DD9ZR01DAU14",
                    "Ec2InstanceId": "i-0736242069217a485",
                    "PublicDnsName": "ec2-34-222-47-14.us-west-2.compute.amazonaws.com",
                    "PublicIpAddress": "34.222.47.14",
                    "PrivateDnsName": "ip-172-31-1-113.us-west-2.compute.internal",
                    "PrivateIpAddress": "172.31.1.113",
                }
            ]
        }
        describe_sec_conf_response = {
            "Name": "kerb-security-config",
            "SecurityConfiguration": '{"EncryptionConfiguration": {"EnableInTransitEncryption": false, '
            '"EnableAtRestEncryption": false},"AuthenticationConfiguration": {'
            '"KerberosConfiguration": {"Provider": "ClusterDedicatedKdc", '
            '"ClusterDedicatedKdcConfiguration": {"TicketLifetimeInHours": 24 }}}}',
        }
        emr_stub.add_response("describe_cluster", describe_cluster_response)
        emr_stub.add_response("list_instances", list_instances_response)
        emr_stub.add_response(
            "describe_security_configuration", describe_sec_conf_response
        )
        emr_cluster = EMRCluster(cluster_id="j-3DD9ZR01DAU14", emr=emr)
        assert emr_cluster.is_krb_cluster
        assert (
            emr_cluster.primary_node_private_dns_name()
            == "ip-172-31-1-113.us-west-2.compute.internal"
        )
        krb_props = {
            "realms": {
                "KTEST.COM": {
                    "kdc": "ip-172-31-1-113.us-west-2.compute.internal:88",
                    "admin_server": "ip-172-31-1-113.us-west-2.compute.internal:749",
                    "default_domain": "test.me",
                }
            },
            "libdefaults": {"default_realm": "KTEST.COM", "ticket_lifetime": "24h"},
            "domain_realm": {
                "test.me": "KTEST.COM",
                ".test.me": "KTEST.COM",
            },
        }
        assert emr_cluster.get_krb_conf() == krb_props
        assert emr_cluster.get_kinit_user_name("ec2-user") == "ec2-user"


@patch("sagemaker_studio_sparkmagic_lib.utils.get_domain_search")
def test_cross_realm_krb_cluster(mock_utils):
    mock_utils.return_value = "test.me"
    emr = boto3.client("emr", region_name="us-west-2")
    with Stubber(emr) as emr_stub:
        describe_cluster_response = {
            "Cluster": {
                "Id": "j-3DD9ZR01DAU14",
                "Name": "Mycluster",
                "SecurityConfiguration": "kerb-security-config",
                "KerberosAttributes": {
                    "Realm": "EC2.INTERNAL",
                    "KdcAdminPassword": "********",
                },
                "MasterPublicDnsName": "ec2-34-222-47-14.us-west-2.compute.amazonaws.com",
            }
        }
        list_instances_response = {
            "Instances": [
                {
                    "Id": "j-3DD9ZR01DAU14",
                    "Ec2InstanceId": "i-0736242069217a485",
                    "PublicDnsName": "ec2-34-222-47-14.us-west-2.compute.amazonaws.com",
                    "PublicIpAddress": "34.222.47.14",
                    "PrivateDnsName": "ip-172-31-1-113.us-west-2.compute.internal",
                    "PrivateIpAddress": "172.31.1.113",
                }
            ]
        }
        describe_sec_conf_response = {
            "Name": "kerb-security-config",
            "SecurityConfiguration": '{"EncryptionConfiguration": {"EnableInTransitEncryption": false, '
            '"EnableAtRestEncryption": false},"AuthenticationConfiguration": {'
            '"KerberosConfiguration": {"Provider": "ClusterDedicatedKdc", '
            '"ClusterDedicatedKdcConfiguration": {"TicketLifetimeInHours": 24,'
            '"CrossRealmTrustConfiguration": {"Realm": "KOLLOJUN.NET","Domain": "kollojun.net",'
            ' "AdminServer": "kollojun.net", "KdcServer": "kollojun.net"}}}}}',
        }
        emr_stub.add_response("describe_cluster", describe_cluster_response)
        emr_stub.add_response("list_instances", list_instances_response)
        emr_stub.add_response(
            "describe_security_configuration", describe_sec_conf_response
        )
        emr_cluster = EMRCluster(cluster_id="j-3DD9ZR01DAU14", emr=emr)
        assert emr_cluster.is_krb_cluster
        assert (
            emr_cluster.primary_node_private_dns_name()
            == "ip-172-31-1-113.us-west-2.compute.internal"
        )
        krb_props = {
            "realms": {
                "EC2.INTERNAL": {
                    "kdc": "ip-172-31-1-113.us-west-2.compute.internal:88",
                    "admin_server": "ip-172-31-1-113.us-west-2.compute.internal:749",
                    "default_domain": "test.me",
                },
                "KOLLOJUN.NET": {
                    "kdc": "kollojun.net",
                    "admin_server": "kollojun.net",
                    "default_domain": "kollojun.net",
                },
            },
            "libdefaults": {"default_realm": "EC2.INTERNAL", "ticket_lifetime": "24h"},
            "domain_realm": {
                "test.me": "EC2.INTERNAL",
                ".test.me": "EC2.INTERNAL",
                ".kollojun.net": "KOLLOJUN.NET",
                "kollojun.net": "KOLLOJUN.NET",
            },
        }
        assert emr_cluster.get_krb_conf() == krb_props
        assert emr_cluster.get_kinit_user_name("ec2-user") == "ec2-user"


@patch("sagemaker_studio_sparkmagic_lib.utils.get_domain_search")
def test_external_kdc_cluster(mock_utils):
    mock_utils.return_value = "test.me"
    emr = boto3.client("emr", region_name="us-west-2")
    with Stubber(emr) as emr_stub:
        describe_cluster_response = {
            "Cluster": {
                "Id": "j-3DD9ZR01DAU14",
                "Name": "Mycluster",
                "SecurityConfiguration": "kerb-security-config",
                "KerberosAttributes": {
                    "ADDomainJoinPassword": "********",
                    "Realm": "MYKERBEROSREALM.COM",
                    "ADDomainJoinUser": "********",
                    "KdcAdminPassword": "********",
                },
                "MasterPublicDnsName": "ec2-34-222-47-14.us-west-2.compute.amazonaws.com",
            }
        }
        list_instances_response = {
            "Instances": [
                {
                    "Id": "j-3DD9ZR01DAU14",
                    "Ec2InstanceId": "i-0736242069217a485",
                    "PublicDnsName": "ec2-34-222-47-14.us-west-2.compute.amazonaws.com",
                    "PublicIpAddress": "34.222.47.14",
                    "PrivateDnsName": "ip-172-31-1-113.us-west-2.compute.internal",
                    "PrivateIpAddress": "172.31.1.113",
                }
            ]
        }
        describe_sec_conf_response = {
            "Name": "kerb-security-config",
            "SecurityConfiguration": '{"AuthenticationConfiguration":{"KerberosConfiguration":'
            '{"ExternalKdcConfiguration":{"KdcServerType":"Single","AdIntegrationConfiguration":'
            '{"AdRealm":"MYADDOMAIN.COM","AdDomain":"myaddomain.com","AdServer":"myaddomain.com"},'
            '"AdminServer":"ip-10-0-0-164.us-west-2.compute.internal",'
            '"KdcServer":"ip-10-0-0-164.us-west-2.compute.internal"}'
            ',"Provider":"ExternalKdc"}}}',
        }
        emr_stub.add_response("describe_cluster", describe_cluster_response)
        emr_stub.add_response("list_instances", list_instances_response)
        emr_stub.add_response(
            "describe_security_configuration", describe_sec_conf_response
        )
        emr_cluster = EMRCluster(cluster_id="j-3DD9ZR01DAU14", emr=emr)
        assert emr_cluster.is_krb_cluster
        assert (
            emr_cluster.primary_node_private_dns_name()
            == "ip-172-31-1-113.us-west-2.compute.internal"
        )
        krb_props = {
            "realms": {
                "MYKERBEROSREALM.COM": {
                    "kdc": "ip-10-0-0-164.us-west-2.compute.internal",
                    "admin_server": "ip-10-0-0-164.us-west-2.compute.internal",
                    "default_domain": "test.me",
                },
                "MYADDOMAIN.COM": {
                    "kdc": "myaddomain.com",
                    "admin_server": "myaddomain.com",
                    "default_domain": "myaddomain.com",
                },
            },
            "libdefaults": {
                "default_realm": "MYKERBEROSREALM.COM",
                "ticket_lifetime": "24h",
            },
            "domain_realm": {
                "test.me": "MYKERBEROSREALM.COM",
                ".test.me": "MYKERBEROSREALM.COM",
                ".myaddomain.com": "MYADDOMAIN.COM",
                "myaddomain.com": "MYADDOMAIN.COM",
            },
        }
        assert emr_cluster.get_krb_conf() == krb_props
        assert emr_cluster.get_kinit_user_name("ec2-user") == "ec2-user@MYADDOMAIN.COM"
        assert emr_cluster.krb_hostname_override() == "ip-172-31-1-113.test.me"

