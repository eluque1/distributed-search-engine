import boto3

bucket = "555-project-crawl"
prefix = "pt-crawl/"

s3 = boto3.client("s3")

with open("manifest.csv", "w") as f:
    f.write("Bucket,Key\n")  # header required

    continuation = None
    while True:
        args = dict(Bucket=bucket, Prefix=prefix)
        if continuation:
            args["ContinuationToken"] = continuation

        resp = s3.list_objects_v2(**args)

        for obj in resp.get("Contents", []):
            f.write(f"{bucket},{obj['Key']}\n")

        if not resp.get("IsTruncated"):
            break

        continuation = resp.get("NextContinuationToken")
