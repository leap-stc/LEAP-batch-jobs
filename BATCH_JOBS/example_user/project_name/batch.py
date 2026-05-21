# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "s3fs",
# ]
# ///

import os
import s3fs

def main():
    fs = s3fs.S3FileSystem(
        key=os.environ["OSN_ACCESS_KEY_INBOX"],
        secret=os.environ["OSN_SECRET_KEY_INBOX"],
        client_kwargs={"endpoint_url": os.environ["OSN_ENDPOINT"]},
    )

    # write a small test file
    path = "leap-pangeo-inbox/sarika/osn_write_test/hello.txt"
    with fs.open(path, "w") as f:
        f.write("OSN write test from LEAP batch job — success!")

    # read it back to confirm
    with fs.open(path, "r") as f:
        content = f.read()

    print(f"Written and read back: {content}")

if __name__ == "__main__":
    main()