import json
from configparser import ConfigParser
from hashlib import sha256
from itertools import islice
from os import environ
from time import time
from urllib.parse import unquote_plus

import boto3
import botocore
from boto3.dynamodb.conditions import Key


s3 = boto3.client("s3")
ObjectVersion = boto3.resource("s3").ObjectVersion


class dotdict(dict):
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(key)

    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


def chunks(xs, n):
    xs = iter(xs)
    while True:
        if chunk := list(islice(xs, n)):
            yield chunk
        else:
            break


parser = ConfigParser()
parser.read(environ["LAMBDA_TASK_ROOT"] + "/config.ini")
config = parser["default"]
BUCKET = boto3.resource("s3").Bucket(config.pop("bucket"))

DYNAMODB = boto3.resource("dynamodb")
TABLE_NAMES = dotdict(**config)


TABLES = dotdict(
    (nickname, DYNAMODB.Table(name)) for nickname, name in TABLE_NAMES.items()
)


class Blob:
    def __init__(self, digest):
        self._digest = digest
        self._s3 = BUCKET.Object("blobs/" + digest)

    def delete(self):
        self._s3.delete()
        TABLES.blobs.delete_item(Key=dict(digest=self._digest))

    def _exists_in_s3(self):
        try:
            self._s3.load()
        except botocore.exceptions.ClientError:
            # In general this will be a 404
            # It could be another type of error, but either way it means the data is
            # inaccessible
            return False

        TABLES.blobs.put_item(Item=dict(digest=self._digest))
        return True

    @classmethod
    def _batch_fetch_dynamodb(cls, digests):
        ret = []
        for chunk in chunks(digests, 100):
            resp = DYNAMODB.batch_get_item(
                RequestItems={
                    TABLE_NAMES.blobs: dict(
                        Keys=[dict(digest=d) for d in digests],
                        ProjectionExpression="digest",
                    )
                }
            )

            ret.extend(row["digest"] for row in resp["Responses"][TABLE_NAMES.blobs])

        return ret

    @classmethod
    def batch_exists(cls, digests):
        exists = set(cls._batch_fetch_dynamodb(digests))
        missing = set(digests) - exists

        for digest in missing:
            if cls(digest)._exists_in_s3():
                exists.add(digest)

        return exists


class Indexers:
    formats = {
        "application/vnd.docker.distribution.manifest.v2+json": "manifest",
        "application/vnd.oci.image.manifest.v1+json": "manifest",
        "application/vnd.oci.image.config.v1+json": "image",
        "application/vnd.docker.container.image.v1+json": "image",
    }

    @staticmethod
    def _manifest(body, name: str):
        digests = [body["config"]["digest"]]
        for layer in body["layers"]:
            digests.append(layer["digest"])

        exists_set = Blob.batch_exists(digests)
        items = [dict(digest=d, source=name) for d in digests]

        # Write in_refs, then out_refs
        # The happens-before relationship is why we don't use a GSI
        # This synchronizes with the delete routine.

        with TABLES.in_references.batch_writer() as batch:
            for item in items:
                batch.put_item(item)

        with TABLES.references.batch_writer() as batch:
            for item in items:
                batch.put_item(dict(found=item["digest"] in exists_set, **item))

        return digests

    @staticmethod
    def _handle_image(body):
        pass

    @classmethod
    def index(cls, manifest, name: str):
        fmt = cls.formats[manifest["mediaType"]]
        op = getattr(cls, "_" + fmt)
        return op(manifest, name)


def trim_start(s, prefix):
    if not s.startswith(prefix):
        return None

    return s[len(prefix) :]


class ManifestHandlers:
    @staticmethod
    def _handle_manifest_created(s3_object, image_name):
        """A manifest (a GC root) was uploaded"""

        repo_name = image_name.split(":")[0]
        body = s3_object.get()["Body"].read()
        manifest = json.loads(body)
        digest = "sha256:" + sha256(body).hexdigest()

        Indexers.index(manifest, image_name)

        TABLES.manifests.put_item(Item=dict(name=image_name))
        TABLES.manifests.put_item(
            Item=dict(name=f"{repo_name}:{digest}", actual=image_name)
        )

        s3.put_object_tagging(
            Bucket=s3_object.bucket_name,
            Key=s3_object.object_key,
            VersionId=s3_object.id,
            Tagging=dict(TagSet=[dict(Key="indexed", Value=str(int(time())))]),
        )

    @staticmethod
    def _put_expires(image_name, *, already_exists: bool):
        # If any indexing attempts to complete (not initiate) they will fail in the
        # presence of this tombstone. Good!
        expires = int(time()) + 3600

        kwargs = dict()
        if not already_exists:
            kwargs.update(
                ConditionExpression="attribute_not_exists(#name)",
                ExpressionAttributeNames={"#name": "name"},
            )

        TABLES.manifests.put_item(
            Item=dict(name=image_name, expires=expires),
            **kwargs,
        )

    @staticmethod
    def _gc_ref(digest, image_name):
        """Garbage-collects a single reference to a blob, deleting the blob if its
        refcount hits zero."""

        TABLES.in_references.delete_item(Key=dict(digest=digest, source=image_name))

        resp = TABLES.in_references.query(
            KeyConditionExpression=Key("digest").eq(digest), Select="COUNT", Limit=1
        )
        if resp["Count"] == 0:
            # The object is now unreferenced
            Blob(digest).delete()

        TABLES.references.delete_item(Key=dict(source=image_name, digest=digest))

    @classmethod
    def _perform_gc(cls, image_name):
        """Garbage collects all of the outbound references of a single manifest."""

        while True:
            resp = TABLES.references.query(
                KeyConditionExpression=Key("source").eq(image_name)
            )

            if resp["Count"] == 0:
                break

            for item in resp["Items"]:
                cls._gc_ref(item["digest"], image_name)

    @classmethod
    def _handle_manifest_deleted(cls, s3_object, image_name):
        """A manifest was deleted. Perform garbage collection."""

        resp = TABLES.manifests.get_item(Key=dict(name=image_name))
        item = resp.get("Item")
        if item:
            cls._perform_gc(image_name)

        # If this fails, a create happened during this delete
        cls._put_expires(image_name, already_exists=bool(item))

        s3.put_object_tagging(
            Bucket=s3_object.bucket_name,
            Key=s3_object.object_key,
            VersionId=s3_object.id,
            Tagging=dict(TagSet=[dict(Key="deindexed", Value=str(int(time())))]),
        )

    @classmethod
    def _determine_op(cls, event_type):
        if event_type == "ObjectRemoved:DeleteMarkerCreated":
            return cls._handle_manifest_deleted

        if event_type.startswith("ObjectCreated:"):
            return cls._handle_manifest_created

        return None

    @classmethod
    def handle(cls, event_type, s3_object, image_name):
        op = cls._determine_op(event_type)
        if op:
            return op(s3_object, image_name)


def handle_record(r):
    if r["eventSource"] != "aws:s3":
        return

    s3_info = r["s3"]
    bucket = s3_info["bucket"]["name"]
    if bucket != BUCKET.name:
        # We got sent a notification for the wrong bucket!?
        return

    object_info = s3_info["object"]
    key = unquote_plus(object_info["key"], encoding="utf-8")
    image_name = trim_start(key, "manifests/")

    if not image_name:
        # We got a notification about something other than a manifest
        return

    s3_object = ObjectVersion(bucket, key, object_info["versionId"])
    ManifestHandlers.handle(r["eventName"], s3_object, image_name)


def lambda_handler(event, context):
    for r in event["Records"]:
        handle_record(r)
