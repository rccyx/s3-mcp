import boto3
from typing import Any, Dict, Optional, List, Union
from fastmcp import FastMCP, Context
from env import env

s3_client = boto3.client(
    "s3",
    aws_access_key_id=env.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=env.AWS_SECRET_ACCESS_KEY,
    region_name=env.AWS_REGION,
)

app: FastMCP = FastMCP("s3")


@app.tool(name="list_buckets", description="List all buckets")
async def list_buckets(context: Context) -> Dict[str, Union[List[str], str]]:
    """
    List all S3 buckets.

    Returns:
        A dictionary containing a list of bucket names or an error message.

    Example:
        {
            "buckets": ["bucket1", "bucket2"]
        }
    """
    try:
        response = s3_client.list_buckets()
        buckets = [bucket["Name"] for bucket in response["Buckets"]]
        return {"buckets": buckets}
    except Exception as e:
        return {"error": str(e)}


@app.tool(
    name="create_bucket",
    description="Create a new S3 bucket with optional security config",
)
async def create_bucket(
    context: Context,
    bucket_name: str,
    region: str = "us-west-1",
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Union[bool, str]]:
    """
    Create a new S3 bucket.

    Args:
        bucket_name (str): The name of the bucket to create.
        region (str): The AWS region where the bucket will be created. Default is "us-west-1".
        config (Optional[Dict[str, Any]]): Optional configuration for the bucket (e.g., blockPublicAccess, versioning, encryption).

    Returns:
        A dictionary indicating success or error.

    Example:
        {
            "success": True,
            "bucket": "my-new-bucket"
        }
    """
    try:
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                "LocationConstraint": region if region != "us-east-1" else None  # type: ignore
            },
        )

        if config:
            if config.get("blockPublicAccess"):
                s3_client.put_public_access_block(
                    Bucket=bucket_name,
                    PublicAccessBlockConfiguration=config["blockPublicAccess"],
                )
            if config.get("versioning"):
                s3_client.put_bucket_versioning(
                    Bucket=bucket_name,
                    VersioningConfiguration={
                        "Status": "Enabled" if config["versioning"] else "Suspended"
                    },
                )
            if config.get("encryption"):
                s3_client.put_bucket_encryption(
                    Bucket=bucket_name,
                    ServerSideEncryptionConfiguration={
                        "Rules": [
                            {
                                "ApplyServerSideEncryptionByDefault": {
                                    "SSEAlgorithm": config["encryption"]
                                }
                            }
                        ]
                    },
                )
        return {"success": True, "bucket": bucket_name}
    except Exception as e:
        return {"error": str(e)}


@app.tool(name="list_bucket", description="List objects in a bucket")
async def list_bucket(
    context: Context, bucket_name: str, key_prefix: str = ""
) -> Dict[str, Union[str, List[Dict[str, Any]]]]:
    """
    List objects in a specified S3 bucket.

    Args:
        bucket_name (str): The name of the bucket.
        key_prefix (str): Optional prefix to filter the objects.

    Returns:
        A dictionary containing the bucket name and a list of files or an error message.

    Example:
        {
            "bucket": "my-bucket",
            "files": [
                {"key": "file1.txt", "size": 1234, "last_modified": "2023-01-01T00:00:00"},
                {"key": "file2.txt", "size": 5678, "last_modified": "2023-01-02T00:00:00"}
            ]
        }
    """
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=key_prefix)
        files = []
        if "Contents" in response:
            for obj in response["Contents"]:
                files.append(
                    {
                        "key": obj["Key"],
                        "size": obj["Size"],
                        "last_modified": obj["LastModified"].isoformat(),
                    }
                )
        return {"bucket": bucket_name, "files": files}
    except Exception as e:
        return {"error": str(e)}


@app.tool(name="get_object", description="Get an object from a bucket")
async def get_object(
    context: Context, bucket_name: str, key: str
) -> Union[str, Dict[str, str]]:
    """
    Retrieve an object from a specified S3 bucket.

    Args:
        bucket_name (str): The name of the bucket.
        key (str): The key of the object to retrieve.

    Returns:
        The content of the object or an error message.

    Example:
        "This is the content of the object."
    """
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        return response["Body"].read().decode("utf-8")
    except Exception as e:
        return {"error": str(e)}


@app.tool(name="put_object", description="Put an object into a bucket")
async def put_object(
    context: Context, bucket_name: str, key: str, body: str
) -> Dict[str, Union[bool, str]]:
    """
    Upload an object to a specified S3 bucket.

    Args:
        bucket_name (str): The name of the bucket.
        key (str): The key for the object.
        body (str): The content of the object.

    Returns:
        A dictionary indicating success.

    Example:
        {
            "success": True
        }
    """
    try:
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=body)
        return {"success": True}
    except Exception as e:
        return {"error": str(e)}


@app.tool(name="upload_local_file", description="Upload a local file to a bucket")
async def upload_local_file(
    context: Context, bucket_name: str, local_path: str, key: str
) -> Dict[str, Union[bool, str]]:
    """
    Upload a local file to a specified S3 bucket.

    Args:
        bucket_name (str): The name of the bucket.
        local_path (str): The path to the local file.
        key (str): The key for the object in S3.

    Returns:
        A dictionary indicating success.

    Example:
        {
            "success": True
        }
    """
    try:
        s3_client.upload_file(local_path, bucket_name, key)
        return {"success": True}
    except Exception as e:
        return {"error": str(e)}


@app.tool(
    name="download_file_to_local",
    description="Download a file from a bucket to a local path",
)
async def download_file_to_local(
    context: Context, bucket_name: str, key: str, local_path: str
) -> Dict[str, Union[bool, str]]:
    """
    Download a file from a specified S3 bucket to a local path.

    Args:
        bucket_name (str): The name of the bucket.
        key (str): The key of the object to download.
        local_path (str): The local path where the file will be saved.

    Returns:
        A dictionary indicating success.

    Example:
        {
            "success": True
        }
    """
    try:
        s3_client.download_file(bucket_name, key, local_path)
        return {"success": True}
    except Exception as e:
        return {"error": str(e)}


@app.tool(name="delete_object", description="Delete an object from a bucket")
async def delete_object(
    context: Context, bucket_name: str, key: str
) -> Dict[str, Union[bool, str]]:
    """
    Delete an object from a specified S3 bucket.

    Args:
        bucket_name (str): The name of the bucket.
        key (str): The key of the object to delete.

    Returns:
        A dictionary indicating success.

    Example:
        {
            "success": True
        }
    """
    try:
        s3_client.delete_object(Bucket=bucket_name, Key=key)
        return {"success": True}
    except Exception as e:
        return {"error": str(e)}


@app.tool(
    name="generate_presigned_url",
    description="Generate a presigned URL for accessing or uploading an object",
)
async def generate_presigned_url(
    context: Context,
    bucket_name: str,
    key: str,
    expires_in: int = 3600,
    http_method: str = "GET",
) -> Dict[str, Union[bool, str]]:
    """
    Generate a presigned URL for accessing or uploading an object.

    Args:
        bucket_name (str): The name of the bucket.
        key (str): The key of the object.
        expires_in (int): The expiration time in seconds for the presigned URL. Default is 3600 seconds.
        http_method (str): The HTTP method to use ("GET" or "PUT"). Default is "GET".

    Returns:
        A dictionary containing the presigned URL or an error message.

    Example:
        {
            "success": True,
            "url": "https://presigned-url"
        }
    """
    try:
        url = s3_client.generate_presigned_url(
            ClientMethod="get_object" if http_method.upper() == "GET" else "put_object",
            Params={"Bucket": bucket_name, "Key": key},
            ExpiresIn=expires_in,
        )
        return {"success": True, "url": url}
    except Exception as e:
        return {"error": str(e)}


@app.tool(name="put_bucket_policy", description="Set or update a bucket policy")
async def put_bucket_policy(
    context: Context, bucket_name: str, policy_json: str
) -> Dict[str, Union[bool, str]]:
    """
    Set or update the policy for a specified S3 bucket.

    Args:
        bucket_name (str): The name of the bucket.
        policy_json (str): A valid JSON string representing the policy.

    Returns:
        A dictionary indicating success.

    Example:
        {
            "success": True
        }
    """
    try:
        s3_client.put_bucket_policy(Bucket=bucket_name, Policy=policy_json)
        return {"success": True}
    except Exception as e:
        return {"error": str(e)}


@app.tool(name="get_bucket_policy", description="Retrieve the current bucket policy")
async def get_bucket_policy(
    context: Context, bucket_name: str
) -> Dict[str, Union[bool, str]]:
    """
    Retrieve the current policy for a specified S3 bucket.

    Args:
        bucket_name (str): The name of the bucket.

    Returns:
        A dictionary containing the bucket policy or an error message.

    Example:
        {
            "success": True,
            "policy": "policy-json-string"
        }
    """
    try:
        response = s3_client.get_bucket_policy(Bucket=bucket_name)
        return {"success": True, "policy": response["Policy"]}
    except Exception as e:
        return {"error": str(e)}


@app.tool(name="delete_bucket_policy", description="Delete the current bucket policy")
async def delete_bucket_policy(
    context: Context, bucket_name: str
) -> Dict[str, Union[bool, str]]:
    """
    Delete the current policy for a specified S3 bucket.

    Args:
        bucket_name (str): The name of the bucket.

    Returns:
        A dictionary indicating success.

    Example:
        {
            "success": True
        }
    """
    try:
        s3_client.delete_bucket_policy(Bucket=bucket_name)
        return {"success": True}
    except Exception as e:
        return {"error": str(e)}


@app.tool(name="delete_bucket", description="Delete an empty S3 bucket")
async def delete_bucket(
    context: Context, bucket_name: str
) -> Dict[str, Union[bool, str]]:
    """
    Delete an empty S3 bucket.

    Args:
        bucket_name (str): The name of the bucket to delete.

    Returns:
        A dictionary indicating success or error.

    Example:
        {
            "success": True
        }
    """
    try:
        s3_client.delete_bucket(Bucket=bucket_name)
        return {"success": True}
    except Exception as e:
        return {"error": str(e)}


@app.tool(name="copy_object", description="Copy an object from one location to another")
async def copy_object(
    context: Context,
    source_bucket: str,
    source_key: str,
    dest_bucket: str,
    dest_key: str,
) -> Dict[str, Union[bool, str]]:
    """
    Copy an object from one location to another within S3.

    Args:
        source_bucket (str): Source bucket name
        source_key (str): Source object key
        dest_bucket (str): Destination bucket name
        dest_key (str): Destination object key

    Returns:
        A dictionary indicating success or error.

    Example:
        {
            "success": True,
            "copy_id": "copy-operation-id"
        }
    """
    try:
        response = s3_client.copy_object(
            CopySource={"Bucket": source_bucket, "Key": source_key},
            Bucket=dest_bucket,
            Key=dest_key,
        )
        return {"success": True, "copy_id": response["CopyObjectResult"]["ETag"]}
    except Exception as e:
        return {"error": str(e)}


@app.tool(name="get_bucket_lifecycle", description="Get bucket lifecycle configuration")
async def get_bucket_lifecycle(
    context: Context, bucket_name: str
) -> Dict[str, Union[List[Dict[str, Any]], str]]:
    """
    Get the lifecycle configuration of a bucket.

    Args:
        bucket_name (str): The name of the bucket

    Returns:
        A dictionary containing lifecycle rules or error message.

    Example:
        {
            "rules": [
                {
                    "ID": "Move to IA after 30 days",
                    "Status": "Enabled",
                    "Transitions": [{"Days": 30, "StorageClass": "STANDARD_IA"}]
                }
            ]
        }
    """
    try:
        response = s3_client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
        # Convert LifecycleRuleOutputTypeDef to Dict[str, Any]
        rules = [{k: v for k, v in rule.items()} for rule in response["Rules"]]
        return {"rules": rules}
    except Exception as e:
        return {"error": str(e)}


@app.tool(name="put_bucket_lifecycle", description="Set bucket lifecycle configuration")
async def put_bucket_lifecycle(
    context: Context, bucket_name: str, lifecycle_config: List[Dict[str, Any]]
) -> Dict[str, Union[bool, str]]:
    """
    Set lifecycle configuration for a bucket.

    Args:
        bucket_name (str): The name of the bucket
        lifecycle_config (List[Dict[str, Any]]): Lifecycle configuration rules

    Returns:
        A dictionary indicating success or error.

    Example:
        {
            "success": True
        }
    """
    try:
        formatted_rules = []
        for rule in lifecycle_config:
            formatted_rule = {
                "ID": rule.get("ID"),
                "Status": rule.get("Status"),
                "Transitions": rule.get("Transitions", []),
                "Expiration": rule.get("Expiration"),
                "NoncurrentVersionExpiration": rule.get("NoncurrentVersionExpiration"),
                "NoncurrentVersionTransitions": rule.get(
                    "NoncurrentVersionTransitions", []
                ),
            }
            formatted_rules.append(formatted_rule)

        s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration={
                "Rules": formatted_rules  # type: ignore
            },
        )
        return {"success": True}
    except Exception as e:
        return {"error": str(e)}


@app.tool(name="get_object_tagging", description="Get object tags")
async def get_object_tagging(
    context: Context, bucket_name: str, key: str
) -> Dict[str, Union[List[Dict[str, str]], str]]:
    """
    Get tags for an S3 object.

    Args:
        bucket_name (str): The name of the bucket
        key (str): The object key

    Returns:
        A dictionary containing object tags or error message.

    Example:
        {
            "tags": [
                {"Key": "project", "Value": "demo"},
                {"Key": "environment", "Value": "production"}
            ]
        }
    """
    try:
        response = s3_client.get_object_tagging(Bucket=bucket_name, Key=key)
        tags = [
            {"Key": tag["Key"], "Value": tag["Value"]} for tag in response["TagSet"]
        ]
        return {"tags": tags}
    except Exception as e:
        return {"error": str(e)}


@app.tool(name="put_object_tagging", description="Set object tags")
async def put_object_tagging(
    context: Context, bucket_name: str, key: str, tags: List[Dict[str, str]]
) -> Dict[str, Union[bool, str]]:
    """
    Set tags for an S3 object.

    Args:
        bucket_name (str): The name of the bucket
        key (str): The object key
        tags (List[Dict[str, str]]): List of tag dictionaries with Key and Value

    Returns:
        A dictionary indicating success or error.

    Example:
        {
            "success": True
        }
    """
    try:
        formatted_tags = [{"Key": tag["Key"], "Value": tag["Value"]} for tag in tags]

        s3_client.put_object_tagging(
            Bucket=bucket_name,
            Key=key,
            Tagging={"TagSet": formatted_tags},  # type: ignore
        )
        return {"success": True}
    except Exception as e:
        return {"error": str(e)}


@app.tool(name="get_bucket_cors", description="Get bucket CORS configuration")
async def get_bucket_cors(
    context: Context, bucket_name: str
) -> Dict[str, Union[List[Dict[str, Any]], str]]:
    """
    Get CORS configuration for a bucket.

    Args:
        bucket_name (str): The name of the bucket

    Returns:
        A dictionary containing CORS rules or error message.

    Example:
        {
            "cors_rules": [
                {
                    "AllowedHeaders": ["*"],
                    "AllowedMethods": ["GET", "PUT"],
                    "AllowedOrigins": ["https://example.com"],
                    "MaxAgeSeconds": 3000
                }
            ]
        }
    """
    try:
        response = s3_client.get_bucket_cors(Bucket=bucket_name)
        cors_rules = [{k: v for k, v in rule.items()} for rule in response["CORSRules"]]
        return {"cors_rules": cors_rules}
    except Exception as e:
        return {"error": str(e)}


def main():
    """Entry point for s3-mcp CLI."""
    app.run(transport="stdio")


if __name__ == "__main__":
    main()
