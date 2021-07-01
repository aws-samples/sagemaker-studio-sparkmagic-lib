from unittest.mock import mock_open, patch

from sagemaker_studio_sparkmagic_lib import utils


def test_get_domain_search_without_search_line():
    with patch("builtins.open", mock_open(read_data="data")) as mock_file:
        result = utils.get_domain_search("us-west-2")
        mock_file.assert_called_with("/etc/resolv.conf", "r")
        assert result == utils.get_default_domain_search("us-west-2")


def test_get_domain_search_happy_case():
    resolv_contents = (
        "# comment\n"
        "search eu-west-1.user.test\n"
        "nameserver 10.0.0.145\n"
        "nameserver 10.0.0.2\n"
        "options timeout:2 attempts:5"
    )

    with patch("builtins.open", mock_open(read_data=resolv_contents)) as mock_file:
        result = utils.get_domain_search("eu-west-1")
        mock_file.assert_called_with("/etc/resolv.conf", "r")
        assert result == "eu-west-1.user.test"


@patch("builtins.open")
def test_get_domain_search_resolv_does_not_exist(mock_open):
    mock_open.side_effect = FileNotFoundError
    result = utils.get_domain_search("eu-west-1")
    mock_open.assert_called_with("/etc/resolv.conf", "r")
    assert result == utils.get_default_domain_search("eu-west-1")


def test_get_emr_endpoint_url():
    assert (
        utils.get_emr_endpoint_url("us-west-2")
        == "https://elasticmapreduce.us-west-2.amazonaws.com"
    )
    assert (
        utils.get_emr_endpoint_url("cn-north-1")
        == "https://elasticmapreduce.cn-north-1.amazonaws.com.cn"
    )
    assert (
        utils.get_emr_endpoint_url("eu-west-1")
        == "https://elasticmapreduce.eu-west-1.amazonaws.com"
    )
    assert (
        utils.get_emr_endpoint_url("us-gov-east-1")
        == "https://elasticmapreduce.us-gov-east-1.amazonaws.com"
    )
