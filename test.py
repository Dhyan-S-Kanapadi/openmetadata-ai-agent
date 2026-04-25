import base64
import os

import requests
from dotenv import load_dotenv


load_dotenv()

OPENMETADATA_URL = os.getenv(
    "OPENMETADATA_URL", "https://sandbox.open-metadata.org/api/v1"
)
OPENMETADATA_JWT_TOKEN = os.getenv("OPENMETADATA_JWT_TOKEN")
OPENMETADATA_EMAIL = os.getenv("OPENMETADATA_EMAIL")
OPENMETADATA_PASSWORD = os.getenv("OPENMETADATA_PASSWORD")


def build_headers(token: str | None = None) -> dict:
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def get_token() -> str | None:
    if OPENMETADATA_JWT_TOKEN:
        print("Using OpenMetadata token from OPENMETADATA_JWT_TOKEN")
        return OPENMETADATA_JWT_TOKEN

    if not OPENMETADATA_EMAIL or not OPENMETADATA_PASSWORD:
        print(
            "No OpenMetadata credentials found. Set OPENMETADATA_JWT_TOKEN "
            "or OPENMETADATA_EMAIL and OPENMETADATA_PASSWORD in .env."
        )
        return None

    encoded_password = base64.b64encode(
        OPENMETADATA_PASSWORD.encode("utf-8")
    ).decode("utf-8")

    res = requests.post(
        f"{OPENMETADATA_URL}/users/login",
        json={
            "email": OPENMETADATA_EMAIL,
            "password": encoded_password,
        },
        headers=build_headers(),
        timeout=20,
    )

    print(f"Login status: {res.status_code}")

    if res.status_code != 200:
        print(f"Login failed: {res.text}")
        if res.status_code == 403:
            print(
                "This OpenMetadata server does not allow password login with "
                "the configured authenticator. Use OPENMETADATA_JWT_TOKEN instead."
            )
        return None

    return res.json().get("accessToken")


def fetch_tables(token: str | None) -> None:
    res = requests.get(
        f"{OPENMETADATA_URL}/tables",
        headers=build_headers(token),
        params={"limit": 5, "include": "all"},
        timeout=20,
    )

    print(f"Tables status: {res.status_code}")

    if res.status_code != 200:
        print(f"Table fetch failed: {res.text}")
        return

    data = res.json()
    tables = data.get("data", [])
    print(f"Number of tables: {len(tables)}")
    for table in tables:
        print(f"- {table.get('fullyQualifiedName')}")


def main() -> None:
    print("Testing OpenMetadata connection...")
    token = get_token()
    if token:
        print("Token acquired from configuration.")
    fetch_tables(token)


if __name__ == "__main__":
    main()
