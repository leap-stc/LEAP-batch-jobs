import json
import urllib.request
from datetime import datetime, timedelta, timezone


def gcp_credential_provider() -> dict:
    """Fetch a GCP OAuth2 token from the instance metadata server.

    Use as the credential_provider for obstore GCSStore on GCP VMs when
    Workload Identity Federation (external_account) credentials are present,
    which obstore does not support natively.
    """
    req = urllib.request.Request(
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
        headers={"Metadata-Flavor": "Google"},
    )
    with urllib.request.urlopen(req) as response:
        data = json.loads(response.read())
    expires_at = datetime.now(tz=timezone.utc) + timedelta(
        seconds=data["expires_in"] - 60
    )
    return {"token": data["access_token"], "expires_at": expires_at}
