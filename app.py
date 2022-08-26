import json
import re
from base64 import b64encode
from configparser import ConfigParser
from os import environ
from traceback import format_exc

import boto3

s3_client = boto3.client("s3")

parser = ConfigParser()
parser.read(environ["LAMBDA_TASK_ROOT"] + "/config.ini")
config = parser["default"]
BUCKET_NAME = config["bucket"]

MATCHER = re.compile("v2/(.*)/(manifests|blobs)/([^/]+)$")

MANIFESTS_TABLE = boto3.resource("dynamodb").Table(config["manifests"])


def _get_actual_manifest(repository, image):
    name = f"{repository}:{image}"

    # This query validates that the manifest has been indexed
    # If it hasn't, the query will return no items and we'll
    # throw an exception.
    response = MANIFESTS_TABLE.get_item(Key=dict(name=name))
    try:
        item = response["Item"]
    except KeyError:
        return None

    if "actual" in item:
        return item["actual"]

    return name


def make_response(status, *, headers=None, body=b"", content_type=None):
    if headers is None:
        headers = dict()

    if content_type is not None:
        headers.setdefault("Content-Type", content_type)

    if not isinstance(body, (str, bytes)):
        body = json.dumps(body)
        headers.setdefault("Content-Type", "application/json")

    if not isinstance(body, bytes):
        body = body.encode()

    headers.setdefault("Content-Type", "text/plain")
    return dict(
        statusCode=status, headers=headers, body=b64encode(body), isBase64Encoded=True
    )


class App:
    def __init__(self, method, path):
        self._method = method
        self._path = path

    @staticmethod
    def route_manifests(repository, image):
        name = _get_actual_manifest(repository, image)
        if name is None:
            return make_response(404, body="Unknown image")

        body = s3_client.get_object(
            Bucket=BUCKET_NAME,
            Key=f"manifests/{name}",
        )["Body"].read()
        media_type = json.loads(body)["mediaType"]
        return make_response(200, body=body, content_type=media_type)

    def route_blobs(self, repository, digest):
        path = "blobs/" + digest

        url = s3_client.generate_presigned_url(
            f"{self._method.lower()}_object",
            Params=dict(Bucket=BUCKET_NAME, Key=path),
            ExpiresIn=900,
        )
        return make_response(302, body="Redirect", headers={"Location": url})

    def route(self):
        if self._method not in ("GET", "HEAD"):
            return make_response(405, "Method Not Allowed")

        m = MATCHER.match(self._path[1:])
        if not m:
            return make_response(404, body="Not Found")

        repository, action, suffix = m.groups()
        route = getattr(self, "route_" + action)
        return route(repository, suffix)


def lambda_handler(event, context):
    try:
        return App(
            event["requestContext"]["http"]["method"],
            event["requestContext"]["http"]["path"],
        ).route()
    except Exception:
        body = format_exc() if config["debug"] == "true" else "Internal Server Error"
        return make_response(500, body=body)
