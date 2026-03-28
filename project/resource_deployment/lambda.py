# Operations to manage Lambda infrastructure
import boto3


def lambda_create(
    lambda_client: boto3.client,
    lambda_zip_path: str,
    lambda_name: str,
    role: str,
    runtime: str,
    handler_name: str,
    tags: dict = None,
    timeout: int = None,
    memory_size: int = None,
    environment_variables: dict = None
) -> dict:
    """
    Creates a lambda function from zipped code

    Args:
        lambda_client (boto3.client): a boto3 lambda client instance
        lambda_zip_path (str): the local path of the zipped code
        lambda_name (str): name of the lambda function
        role (str): role with which the lambda executes
        runtime (str): the runtime of the lambda function
        handler_name (str): name of the lambda handler function
        tags (dict, optional): tags to be added to the lambda function. Defaults to None.
        timeout (int, optional): timeout of the lambda function. Defaults to None.
        memory_size (int, optional): memory size of the lambda function. Defaults to None.
        environment_variables (dict, optional): environment variables to be accessible in the lambda. Defaults to None.

    Returns:
        dict: reponse of the creation
    """
    # --- get zipped binary of code
    with open(lambda_zip_path, "rb") as f:
        zipped_code = f.read()

    # --- create specification dict to add params to pass into creation
    # --- other params are optional
    instance_specification = {
        "FunctionName": lambda_name,
        "Runtime": runtime,
        "Role": role,
        "Handler": handler_name,
        "Code": {"ZipFile": zipped_code},
    }
    # OPTIONALS ------------------
    # --- add tags if given
    if tags:
        instance_specification["Tags"] = tags

    # --- add memory size if given
    if memory_size:
        instance_specification["MemorySize"] = memory_size

    # --- add timeout if given
    if timeout:
        instance_specification["Timeout"] = timeout

    # --- add timeout if given
    if environment_variables:
        instance_specification["Environment"] = {"Variables": environment_variables}

    response = lambda_client.create_function(**instance_specification)

    waiter = lambda_client.get_waiter("function_active")
    waiter.wait(FunctionName=lambda_name)

    return response


def lambda_delete(lambda_client: boto3.client, function_name: str) -> dict:
    """
    Delete the lambda function

    Args:
        lambda_client (boto3.client): a boto3 lambda client instance
        function_name (str): the name of the lambda function

    Returns:
        dict: the deletion response
    """

    return lambda_client.delete_function(FunctionName=function_name)


def lambda_describe(lambda_client: boto3.client, function_name: str) -> dict:
    """
    Describes an existing lambda function

    Args:
        lambda_client (boto3.client): a boto3 lambda client instance
        function_name (str): the name of the lambda function

    Returns:
        dict: the description of the lambda function
    """

    return lambda_client.get_function(FunctionName=function_name)


def lambda_create_sqs_trigger(
    lambda_client: boto3.client, sqs_arn: str, function_name: str
) -> dict:
    """
    Creates an sqs trigger for a lambda function

    Args:
        lambda_client (boto3.client): a boto3 lambda client instance
        sqs_arn (str): the arn of the sqs queue
        function_name (str): the name of the lambda function

    Returns:
        dict: the creation response
    """

    response = lambda_client.create_event_source_mapping(
        EventSourceArn=sqs_arn, FunctionName=function_name
    )

    return response


def lambda_create_dbstream_trigger(
    lambda_client: boto3.client, stream_arn: str, function_name: str
) -> dict:
    """
    Creates a dynamodb stream trigger for a lambda function
    Only gets the single latest record

    Args:
        lambda_client (boto3.client): a boto3 lambda client instance
        stream_arn (str): the arn of the dynamodb stream
        function_name (str): the name of the lambda function

    Returns:
        dict: the creation response
    """
    response = lambda_client.create_event_source_mapping(
        EventSourceArn=stream_arn,
        FunctionName=function_name,
        StartingPosition="LATEST",
        BatchSize=1
    )
    return response
