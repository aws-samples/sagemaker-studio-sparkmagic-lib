## SageMaker SparkMagic Library

[![Version](https://img.shields.io/pypi/v/sagemaker-studio-sparkmagic-lib.svg)](https://pypi.org/project/sagemaker-studio-sparkmagic-lib/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)


This is a CLI tool for generating configuration of SparkMagic, Kerberos required  to connect to EMR cluster. In particular, it generates following two files

1. **SparkMagic Config**: This config file contains information needed to connect SparkMagic kernel's running on studio to Livy application running on EMR. CLI obtains EMR cluster details like Ip address etc.. by describing EMR cluster

2. **Krb5.conf**: If EMR cluster uses kerberos security configuration, this library also generates krb5.conf needed for user authentication on studio 

### Usage

This CLI tool comes pre-installed on Studio SparkMagic Image. It can be used from any notebook created from that image. 

#### Connecting to non-kerberos cluster: 
In a notebook cell, execute following commands

```
%local

!sm-sparkmagic connect --cluster-id "j-xxxxxxxxx"
```

sample output:

```
Successfully read emr cluster(j-xxxxxxxx) details
SparkMagic config file is written to location /etc/sparkmagic/config.json
Completed setting up configuration files for SparkMagic to connect to EMR cluster j-xxxxxxxx


Please complete following steps to complete the connection
1. Restart kernel to complete your setup. This is required so SparkMagic can pickup generated configuration
```

#### Connecting to kerberos cluster: 

It's very similar to non-kerberos cluster, except you can pass 

```
!sm-sparkmagic connect --cluster-id "j-xxxxxxxx" --user-name "ec2-user"
```

sample output:

```
Please follow below steps to complete the setup:
1. Please open image terminal and run 'kinit ec2-user'(user_name: ec2-user) to get kerberos ticket
2. Restart kernel to complete your setup. This is required so SparkMagic can pickup generated configuration
```

#### Connecting to EMR cluster in another account 
To setup configuration for EMR cluster in another account, run following command

```
%local

!sm-sparkmagic connect --cluster-id "j-xxxxx" --role-arn "arn:aws:iam::222222222222:role/role-on-emr-cluster-account"
```

### FAQ
* Can I connect to multiple clusters at same time?
  * You can only connect to one cluster at a time. Tool generates configuration needed to connect to one cluster. If you want to connect to different cluster, one has to re-execute the command providing different cell
* Can I use this CLI on non-SparkMagic image on studio?
  * This cli only comes pre-installed on SparkMagic Image. One can install on any other image if needed
* Can I use this library on SageMaker Notebook instances?
  * It does not come installed on Notebooks either, but you can install and try using it. You may have to relocate SparkMagic conf file
  

### Installing
Install the CLI using pip. 

```
pip install sagemaker-studio-sparkmagic-lib
```

Following extra permissions are required on the role to be able to describe cluster
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "elasticmapreduce:DescribeCluster",
                "elasticmapreduce:DescribeSecurityConfiguration",
                "elasticmapreduce:ListInstances"
            ],
            "Resource": "arn:aws:elasticmapreduce:*:*:cluster/*"
        }
    ]
}
```

### Development

* checkout the repository, and install locally

```
make install
```

* To test locally, you can start python3 REPL and run following python code

```python
import sagemaker_studio_sparkmagic_lib.sparkmagic as sm
sm.connect_to_emr_cluster(cluster_id= "j-xxx", user_name="ec2-user", krb_file_override_path="/tmp/krb5.conf",
     spark_magic_override_path="/tmp/config.json", restart_kernel=False)
```

* To test on studio, create a tar ball and install on studio or your custom image accordingly

```
python setup.py sdist
```
## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.


