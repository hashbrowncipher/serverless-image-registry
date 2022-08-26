This is a serverless Docker registry in a box. It consists of:
 * an S3 bucket for storage and writes
 * a lambda that serves HTTPS for reads
 * a lambda that handles S3 event notifiations for indexing
 * some DynamoDB tables to track metadata

The idea is that an uploader job writes data to two locations in an S3 bucket:

 * `blobs/sha256:<digest>` for content addressed blobs (e.g. image layers)
 * `manifests/<repository>:<tag>` for image manifests

With this approach, a Docker repository becomes a lightweight object, and it is
possible to to have thousands or millions of them. Blobs are reference-counted:
when a blob is no longer referenced by any manifests, it is deleted from the S3
bucket.

The lambda serving HTTPS requests is responsible only for reads (it refuses
writes).  Blob bodies are served by redirecting the client to a signed S3 URL.
The `docker pull` client routines handle redirects transparently.

The infrastructure provisioning templates use Pulumi.

# TODO

* Authentication, ideally copying an existing credential helper
* Gracefully handle manifests that get uploaded before their associated blobs
* Integrate with Cloudfront for read-scalability
