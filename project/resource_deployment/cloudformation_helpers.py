# Operations to manage cloudformation deployments
# Using boto3 so that it can be automated along with other deployments
# Still uses cloudformation templates
import boto3
from typing import Dict, List


def cloudformation_create_stack(
    cloudformation_client: boto3.client,
    stack_name: str,
    template_path: str,
    parameters: Dict = None,
    tags: Dict = None
):
    """
    Deploys a cloudformation stack from a local cloudformation template

    Args:
        cloudformation_client (boto3.client): a boto3 cloudformation client
        stack_name (str): the name of the stack
        template_path (str): the path of the template
        parameters (Dict, optional): key, value of parameters to be passed to the template. Defaults to None.
        tags (Dict, optional): tags (dict, optional): tags to be added to the lambda function. Defaults to None.

    Returns:
        _type_: response of the deployment
    """
    # --- read template file
    with open(template_path, "r") as f:
        template_body = f.read()

    # --- create specification Dict to add params to pass into creation
    # --- other params are optional
    instance_specification = {
        "StackName": stack_name,
        "TemplateBody": template_body,
    }

    # OPTIONALS ------------------
    # --- add parameters if given
    if parameters:
        instance_specification["Parameters"] = [
            {"ParameterKey": k, "ParameterValue": v}
            for k, v in parameters.items()
        ]

    # --- add tags if given
    if tags:
        instance_specification["Tags"] = [
            {"Key": k, "Value": v}
            for k, v in tags.items()
        ]

    response = cloudformation_client.create_stack(**instance_specification)

    # --- wait for completion
    waiter = cloudformation_client.get_waiter("stack_create_complete")
    waiter.wait(StackName=stack_name)

    return response


def cloudformation_delete_stack(
    cloudformation_client: boto3.client,
    stack_name: str
):
    """
    Deletes a cloudformation stack resources

    Args:
        cloudformation_client (boto3.client): a boto3 cloudformation client
        stack_name (str): the name of the stack
    """
    cloudformation_client.delete_stack(StackName=stack_name)

    waiter = cloudformation_client.get_waiter("stack_delete_complete")
    waiter.wait(StackName=stack_name)

def cloudformation_get_outputs(
    cloudformation_client: boto3.client,
    stack_name: str
) -> Dict:
    """
    Returns stack creation outputs

    Args:
        cloudformation_client (boto3.client): a boto3 cloudformation client
        stack_name (str): the name of the stack

    Returns:
        Dict: stack outputs as key, value
    """
    response = cloudformation_client.describe_stacks(StackName=stack_name)
    raw_outputs = response["Stacks"][0].get("Outputs", [])
    outputs = {out["OutputKey"]: out["OutputValue"] for out in raw_outputs}
    return outputs