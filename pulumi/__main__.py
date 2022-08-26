"""An AWS Python Pulumi program"""
import json

import pulumi_aws as aws
from pulumi_aws import dynamodb
from pulumi_aws import iam
from pulumi_aws import lambda_
from pulumi_aws import s3

import pulumi
from pulumi import Output

# bucket_name = RandomString("bucket", length=16, special=False, upper=False)


def output_kwargs(fn):
    def wrapper(args):
        return fn(**args)

    return wrapper


def _make_config_ini(d):
    config_lines = ["[default]"]
    config_lines.extend(f"{k} = {v}" for k, v in d.items())
    config_string = "".join((line + "\n") for line in config_lines)
    return pulumi.StringAsset(config_string)


current = aws.get_caller_identity()
account_id = current.account_id
region = aws.get_region().name


def s3_bucket(identifier):
    bucket = s3.BucketV2(identifier)

    s3.BucketAclV2(identifier, bucket=bucket.bucket, acl="private")

    s3.BucketServerSideEncryptionConfigurationV2(
        identifier,
        bucket=bucket.bucket,
        rules=[
            s3.BucketServerSideEncryptionConfigurationV2RuleArgs(
                apply_server_side_encryption_by_default=s3.BucketServerSideEncryptionConfigurationV2RuleApplyServerSideEncryptionByDefaultArgs(
                    sse_algorithm="AES256",
                ),
                bucket_key_enabled=True,
            )
        ],
    )

    s3.BucketPublicAccessBlock(
        identifier,
        bucket=bucket.bucket,
        block_public_acls=True,
        block_public_policy=True,
        ignore_public_acls=True,
        restrict_public_buckets=True,
    )

    s3.BucketVersioningV2(
        identifier,
        bucket=bucket.bucket,
        versioning_configuration=s3.BucketVersioningV2VersioningConfigurationArgs(
            status="Enabled",
        ),
    )

    s3.BucketLifecycleConfigurationV2(
        identifier,
        bucket=bucket.bucket,
        rules=[
            s3.BucketLifecycleConfigurationV2RuleArgs(
                id="default",
                status="Enabled",
                noncurrent_version_expiration=s3.BucketLifecycleConfigurationV2RuleNoncurrentVersionExpirationArgs(
                    noncurrent_days=1
                ),
                abort_incomplete_multipart_upload=s3.BucketLifecycleConfigurationV2RuleAbortIncompleteMultipartUploadArgs(
                    days_after_initiation=1
                ),
            )
        ],
    )

    return bucket


bucket = s3_bucket("registry")
TABLES = dict()


def _table(name, **kwargs):
    return name, dynamodb.Table(f"registry_{name}", **kwargs)


TABLES = dict(
    [
        _table(
            "references",
            attributes=[
                dynamodb.TableAttributeArgs(
                    name="source",
                    type="S",
                ),
                dynamodb.TableAttributeArgs(
                    name="digest",
                    type="S",
                ),
            ],
            hash_key="source",
            range_key="digest",
            billing_mode="PAY_PER_REQUEST",
        ),
        _table(
            "in_references",
            attributes=[
                dynamodb.TableAttributeArgs(
                    name="source",
                    type="S",
                ),
                dynamodb.TableAttributeArgs(
                    name="digest",
                    type="S",
                ),
            ],
            hash_key="digest",
            range_key="source",
            billing_mode="PAY_PER_REQUEST",
        ),
        _table(
            "manifests",
            attributes=[
                dynamodb.TableAttributeArgs(
                    name="name",
                    type="S",
                ),
            ],
            hash_key="name",
            billing_mode="PAY_PER_REQUEST",
        ),
        _table(
            "blobs",
            attributes=[
                dynamodb.TableAttributeArgs(
                    name="digest",
                    type="S",
                ),
            ],
            hash_key="digest",
            billing_mode="PAY_PER_REQUEST",
        ),
    ]
)

TABLE_NAMES = dict((k, v.name) for (k, v) in TABLES.items())


_LAMBDA_ARP = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
)


def lambda_iam_role(identifier, stmts):
    stmts = [
        dict(
            Effect="Allow",
            Action=[
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
            ],
            Resource=[
                f"arn:aws:logs:{region}:{account_id}:log-group:/aws/lambda/{identifier}-*",
                f"arn:aws:logs:{region}:{account_id}:log-group:/aws/lambda/{identifier}-*:*",
            ],
        )
    ] + stmts
    policy = json.dumps(dict(Version="2012-10-17", Statement=stmts))

    return iam.Role(
        identifier,
        assume_role_policy=_LAMBDA_ARP,
        inline_policies=[iam.RoleInlinePolicyArgs(name="policy", policy=policy)],
    )


def registry_iam_role(bucket_arn):
    return lambda_iam_role(
        "registry_s3_events",
        stmts=[
            dict(
                Effect="Allow",
                Action=[
                    "s3:GetObject",
                    "s3:GetObjectVersion",
                    "s3:DeleteObject",
                    "s3:PutObjectVersionTagging",
                    "s3:ListBucket",
                ],
                Resource=[
                    bucket_arn,
                    bucket_arn + "/*",
                ],
            ),
            dict(
                Effect="Allow",
                Action=[
                    "dynamodb:BatchGetItem",
                    "dynamodb:BatchWriteItem",
                    "dynamodb:DeleteItem",
                    "dynamodb:GetItem",
                    "dynamodb:PutItem",
                    "dynamodb:Query",
                ],
                Resource=[
                    f"arn:aws:dynamodb:{region}:{account_id}:table/registry_*",
                ],
            ),
        ],
    )


role = bucket.arn.apply(registry_iam_role)


def s3_lambda():
    archive = Output.all(bucket=bucket.bucket, **TABLE_NAMES).apply(
        lambda names: pulumi.AssetArchive(
            {
                "config.ini": _make_config_ini(names),
                "lambda_function.py": pulumi.FileAsset("../lambda.py"),
            }
        )
    )

    s3_events_function = lambda_.Function(
        "registry_s3_events",
        code=archive,
        role=role.arn,
        architectures=["x86_64"],
        runtime="python3.9",
        handler="lambda_function.lambda_handler",
    )

    lambda_perm = lambda_.Permission(
        "s3",
        action="lambda:InvokeFunction",
        function=s3_events_function.name,
        principal="s3.amazonaws.com",
        source_account=current.account_id,
        source_arn=bucket.arn,
    )

    s3_events_function.name.apply(
        lambda function_name: s3.BucketNotification(
            "registry",
            pulumi.ResourceOptions(depends_on=[lambda_perm]),
            bucket=bucket.bucket,
            lambda_functions=[
                s3.BucketNotificationLambdaFunctionArgs(
                    lambda_function_arn=f"arn:aws:lambda:{region}:{account_id}:function:"
                    + function_name,
                    events=[
                        "s3:ObjectCreated:*",
                        "s3:ObjectRemoved:DeleteMarkerCreated",
                    ],
                    filter_prefix="manifests/",
                )
            ],
        )
    )


def _s3_bucket_arn(name):
    return "arn:aws:s3:::" + name


def registry_server(config):
    identifier = "registry_server"
    bucket_name = config["bucket"]
    config["debug"] = "true"

    archive = pulumi.AssetArchive(
        {
            "config.ini": _make_config_ini(config),
            "lambda_function.py": pulumi.FileAsset("../app.py"),
        }
    )

    role = lambda_iam_role(
        identifier,
        [
            dict(
                Effect="Allow",
                Action="s3:GetObject",
                Resource=[_s3_bucket_arn(bucket_name) + "/*"],
            ),
            dict(
                Effect="Allow",
                Action=["dynamodb:BatchGetItem", "dynamodb:GetItem"],
                Resource=f"arn:aws:dynamodb:{region}:{account_id}:table/registry_*",
            ),
        ],
    )

    function = lambda_.Function(
        identifier,
        code=archive,
        role=role.arn,
        architectures=["x86_64"],
        runtime="python3.9",
        handler="lambda_function.lambda_handler",
    )

    lambda_.FunctionUrl(
        identifier, function_name=function.name, authorization_type="NONE"
    )


s3_lambda()
Output.all(bucket=bucket.id, manifests=TABLE_NAMES["manifests"]).apply(registry_server)
