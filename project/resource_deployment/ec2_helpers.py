# Operations to manage EC2 infrastructure
import boto3
from typing import Dict, List


def get_ami_id(ssm_client: boto3.client, ami_name: str) -> str:
    """
    Gets the latest ami id for a give ami name

    Args:
        ssm_client (boto3.client): a boto3 ssm client instance
        ami_name (str): amazon machine image name

    Returns:
        str: amazon machine image name
    """
    return ssm_client.get_parameter(Name=ami_name)["Parameter"]["Value"]


def ec2_create(
    ec2_client: boto3.client,
    ami_id: str,
    instance_profile_name: str,
    instance_type: str = "t3.micro",
    min_count: int = 1,
    max_count: int = 1,
    disk_size: int = None,
    disk_device_name: str = None,
    tags: Dict = None,
    subnet: str = None,
    security_groups: List[str] = None,
    user_data_script: str = None,
) -> str:
    """
    Creates an EC2 instance in a running state

    Args:
        ec2_client (boto3.client): a boto3 ec2 client instance
        ami_id (str): the id of the ami (amazon machine image)
        instance_profile_name (str): name given to the instance
        instance_type (str, optional): type of ec2 instance. Defaults to "t3.micro".
        min_count (int, optional): minimum number of instances. Defaults to 1.
        max_count (int, optional): maximum number of instances. Defaults to 1.
        disk_size (int, optional): the size of the disk in gb, if given, disk_device_name also needs to be given. Defaults to None, so the aws default will be used.
        disk_device_name (str, optional): the name of the disk, if given, disk_size also needs to be given. Defaults to None, so the aws default will be used.
        tags (Dict, optional): tags to be added to the instance. Defaults to None.
        subnet (str, optional): the subnet id. Defaults to None and so the default subnet of the default vpc will be used.
        security_groups (List[str], optional): List of security group ids. Defaults to None
        user_data_script: (str): binary script to run on the instance on start. Defaults to None

    Raises:
        ValueError: if subnet is not found
        ValueError: if any of the security groups are not found

    Returns:
        str: the instance id
    """
    # --- create specification Dict to add params to pass into creation
    # --- other params are optional
    instance_specification = {
        "ImageId": ami_id,
        "InstanceType": instance_type,
        "IamInstanceProfile": {"Name": instance_profile_name},
        "MinCount": min_count,
        "MaxCount": max_count,
    }

    # OPTIONALS ------------------
    # --- add tags if given
    if tags:
        instance_specification["TagSpecifications"] = [
            {
                "ResourceType": "instance",
                "Tags": [{"Key": key, "Value": value} for key, value in tags.items()],
            }
        ]

    # --- add disk if given
    if disk_size and disk_device_name:
        instance_specification["BlockDeviceMappings"] = [
            {
                "DeviceName": disk_device_name,
                "Ebs": {"VolumeSize": disk_size, "DeleteOnTermination": True},
            }
        ]

    # --- add subnet if given
    if subnet:
        # --- validate subnet exists, else throw
        results = ec2_client.describe_subnets(
            Filters=[{"Name": "subnet-id", "Values": [subnet]}]
        )
        if len(results["Subnets"]) == 0:
            raise ValueError(f"Subnet {subnet} not found")

        # --- add to specification
        instance_specification["SubnetId"] = subnet

    # --- add security groups if given
    if security_groups:
        # --- describe security groups filtering by given group ids
        results = ec2_client.describe_security_groups(GroupIds=security_groups)
        found_ids = [sg["GroupId"] for sg in results["SecurityGroups"]]
        # --- ensure all groups were found, else throw
        missing = set(security_groups) - set(found_ids)
        if len(missing) > 0:
            raise ValueError(f"Could not find security groups: {missing}")

        # --- add to specification
        instance_specification["SecurityGroupIds"] = security_groups

    # --- add user data script if given
    if user_data_script:
        instance_specification["UserData"] = user_data_script

    response = ec2_client.run_instances(**instance_specification)
    instance_id = response["Instances"][0]["InstanceId"]

    waiter = ec2_client.get_waiter("instance_running")
    waiter.wait(InstanceIds=[instance_id])

    return instance_id


def ec2_add_inbound(ec2_client: boto3.client, security_group_id: str):
    """
    Adds inbound rule to security group

    Args:
        ec2_client (boto3.client): a boto3 ec2 client instance
        security_group_id (str): id of the security group
    """
    # --- allow inbound connections
    ec2_client.authorize_security_group_ingress(
        GroupId=security_group_id,
        IpPermissions=[
            {
                "IpProtocol": "tcp",
                "FromPort": 22,
                "ToPort": 22,
                "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
            }
        ],
    )


def ec2_terminate(ec2_client: boto3.client, instance_id: str) -> Dict:
    """
    Terminate an ec2 instance

    Args:
        ec2_client (boto3.client): a boto3 ec2 client instance
        instance_id (str): instance id of the ec2 instance

    Returns:
        Dict: information about the terminated instance
    """
    response = ec2_client.terminate_instances(InstanceIds=[instance_id])
    waiter = ec2_client.get_waiter("instance_terminated")
    waiter.wait(InstanceIds=[instance_id])
    return response


def ec2_start(ec2_client: boto3.client, instance_id: str) -> Dict:
    """
    Start an existing ec2 instance

    Args:
        ec2_client (boto3.client): a boto3 ec2 client instance
        instance_id (str): instance id of the ec2 instance

    Returns:
        Dict: information about the started instance
    """
    response = ec2_client.start_instances(InstanceIds=[instance_id])
    waiter = ec2_client.get_waiter("instance_running")
    waiter.wait(InstanceIds=[instance_id])
    return response


def ec2_stop(ec2_client: boto3.client, instance_id: str) -> Dict:
    """
    Stop a running ec2 instance

    Args:
        ec2_client (boto3.client): a boto3 ec2 client instance
        instance_id (str): instance id of the ec2 instance

    Returns:
        Dict: information about the stopped instance
    """
    response = ec2_client.stop_instances(InstanceIds=[instance_id])
    waiter = ec2_client.get_waiter("instance_stopped")
    waiter.wait(InstanceIds=[instance_id])
    return response


def ec2_describe(ec2_client: boto3.client, instance_id: str) -> Dict:
    """
    Describes the state of an ec2 instance

    Args:
        ec2_client (boto3.client): a boto3 ec2 client instance
        instance_id (str): instance id of the ec2 instance

    Returns:
        Dict: the description of the ec2 instance
    """
    return ec2_client.describe_instances(InstanceIds=[instance_id])
