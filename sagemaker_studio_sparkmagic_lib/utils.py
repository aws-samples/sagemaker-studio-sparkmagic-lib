import logging
import sys
import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))


def get_default_domain_search(region):
    return f"{region}.compute.internal"


def get_emr_endpoint_url(region):
    """
    boto3 url for EMR is not constructed correctly in some regions See https://github.com/boto/botocore/issues/2376
    @TODO: Deprecate after  https://github.com/boto/botocore/issues/2376 is fixed

    This causes issues when customer use Private Link for EMR without internet connections

    As per recommendation we construct EMR endpoints to match https://docs.aws.amazon.com/general/latest/gr/emr.html
    """
    sess = boto3.session.Session()
    # tokenize endpoint url of format https://us-west-2.elasticmapreduce.amazonaws.com and ignore first two tokens
    boto_url_tokens = sess.client("emr", region_name=region)._endpoint.host.split(".")
    return f"https://elasticmapreduce.{region}.{'.'.join(boto_url_tokens[2:])}"


def get_domain_search(region):
    """
    returns domain search by parsing /etc/resolv.conf
    see https://man7.org/linux/man-pages/man5/resolv.conf.5.html
    This value is used in kerberos config

    NOTE: we should ideally read this information from studio metadata file as /etc/resolv.conf may not
    be in all types of environments. Will be done as follow up
    """
    default_search = get_default_domain_search(region)
    file = "/etc/resolv.conf"
    try:
        with open(file, "r") as resolvconf:
            for line in resolvconf.readlines():
                line = line.split("#", 1)[0]
                line = line.rstrip()
                if "search" in line:
                    return line.split()[1]
    except IOError:
        logger.warning(
            f"Unable to read {file}. Using default value for domain search name: {default_search}"
        )

    return default_search
