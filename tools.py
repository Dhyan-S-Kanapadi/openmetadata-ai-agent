import base64
from urllib.parse import quote

import requests

from config import (
    OPENMETADATA_EMAIL,
    OPENMETADATA_JWT_TOKEN,
    OPENMETADATA_PASSWORD,
    OPENMETADATA_URL,
)

MAX_API_LIMIT = 100
DEFAULT_API_LIMIT = 20
DEFAULT_QUALITY_LIMIT = 50
MAX_LINEAGE_DEPTH = 5
DEFAULT_LINEAGE_DEPTH = 2
FAILED_QUALITY_STATUSES = {"failed", "failure"}


def _build_headers(token: str | None = None) -> dict:
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _get_access_token() -> str | None:
    if OPENMETADATA_JWT_TOKEN:
        return OPENMETADATA_JWT_TOKEN

    if not OPENMETADATA_EMAIL or not OPENMETADATA_PASSWORD:
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
        headers=_build_headers(),
        timeout=20,
    )

    if res.status_code != 200:
        raise RuntimeError(f"OpenMetadata login failed: {res.status_code} {res.text}")

    return res.json().get("accessToken")


def _request(method: str, path: str, **kwargs):
    token = _get_access_token()
    headers = _build_headers(token)
    request_headers = kwargs.pop("headers", {})
    headers.update(request_headers)

    res = requests.request(
        method,
        f"{OPENMETADATA_URL}{path}",
        headers=headers,
        timeout=20,
        **kwargs,
    )

    if res.status_code >= 400:
        raise RuntimeError(f"OpenMetadata API error: {res.status_code} {res.text}")

    return res


def _bounded_limit(limit: int | str | None, default: int = DEFAULT_API_LIMIT) -> int:
    try:
        parsed = int(limit) if limit is not None else default
    except (TypeError, ValueError):
        parsed = default
    return max(1, min(parsed, MAX_API_LIMIT))


def _bounded_depth(depth: int | str | None, default: int = DEFAULT_LINEAGE_DEPTH) -> int:
    try:
        parsed = int(depth) if depth is not None else default
    except (TypeError, ValueError):
        parsed = default
    return max(0, min(parsed, MAX_LINEAGE_DEPTH))


def get_tables(limit=DEFAULT_API_LIMIT):
    limit = _bounded_limit(limit)
    res = _request(
        "GET",
        "/tables",
        params={"limit": limit, "include": "all"},
    )
    data = res.json().get("data", [])
    return [
        {
            "name": table.get("fullyQualifiedName"),
            "owner": table.get("owner", {}).get("name") if table.get("owner") else None,
            "description": table.get("description"),
            "tags": [tag["tagFQN"] for tag in table.get("tags", [])],
        }
        for table in data
    ]


def get_unowned_tables():
    tables = get_tables(50)
    return [table for table in tables if not table["owner"]]


def get_undocumented_tables():
    tables = get_tables(50)
    return [table for table in tables if not table["description"]]


def get_pipelines():
    res = _request(
        "GET",
        "/services/ingestionPipelines",
        params={"limit": 20},
    )
    data = res.json().get("data", [])
    return [
        {
            "name": pipeline.get("name"),
            "id": pipeline.get("id"),
            "status": pipeline.get("pipelineStatuses", {}),
        }
        for pipeline in data
    ]


def trigger_pipeline(pipeline_id: str):
    res = _request(
        "POST",
        f"/services/ingestionPipelines/trigger/{pipeline_id}",
    )
    if res.status_code == 200:
        return "Pipeline triggered successfully"
    return f"Failed to trigger pipeline: {res.status_code}"


def _quality_status(test_case: dict) -> str:
    result = test_case.get("testCaseResult") or {}
    return (
        result.get("testCaseStatus")
        or test_case.get("entityStatus")
        or "Unknown"
    )


def _quality_parameters(test_case: dict) -> dict:
    parameters = {}
    for parameter in test_case.get("parameterValues", []):
        name = parameter.get("name")
        if name:
            parameters[name] = parameter.get("value")
    return parameters


def _format_quality_test(test_case: dict) -> dict:
    result = test_case.get("testCaseResult") or {}
    return {
        "id": test_case.get("id"),
        "name": test_case.get("name"),
        "display_name": test_case.get("displayName"),
        "fully_qualified_name": test_case.get("fullyQualifiedName"),
        "entity": test_case.get("entityFQN"),
        "entity_link": test_case.get("entityLink"),
        "description": test_case.get("description"),
        "status": _quality_status(test_case),
        "updated_at": test_case.get("updatedAt"),
        "updated_by": test_case.get("updatedBy"),
        "parameters": _quality_parameters(test_case),
        "result": {
            "timestamp": result.get("timestamp"),
            "passed_rows": result.get("passedRows"),
            "failed_rows": result.get("failedRows"),
            "sample_data": result.get("sampleData"),
        },
    }


def get_quality_tests(limit=DEFAULT_QUALITY_LIMIT, status: str | None = None):
    limit = _bounded_limit(limit, default=DEFAULT_QUALITY_LIMIT)
    res = _request(
        "GET",
        "/dataQuality/testCases",
        params={"limit": limit, "fields": "testCaseResult"},
    )
    data = res.json().get("data", [])
    tests = [_format_quality_test(test_case) for test_case in data]

    if status:
        expected = status.casefold()
        tests = [
            test_case
            for test_case in tests
            if str(test_case.get("status", "")).casefold() == expected
        ]

    return tests


def get_quality_failures(limit=DEFAULT_QUALITY_LIMIT):
    tests = get_quality_tests(limit=limit)
    return [
        test_case
        for test_case in tests
        if str(test_case.get("status", "")).casefold() in FAILED_QUALITY_STATUSES
    ]


def get_quality_summary(limit=DEFAULT_QUALITY_LIMIT):
    tests = get_quality_tests(limit=limit)
    status_counts = {}
    for test_case in tests:
        status = test_case.get("status") or "Unknown"
        status_counts[status] = status_counts.get(status, 0) + 1

    failed_tests = [
        test_case
        for test_case in tests
        if str(test_case.get("status", "")).casefold() in FAILED_QUALITY_STATUSES
    ]

    return {
        "total_tests": len(tests),
        "status_counts": status_counts,
        "failed_tests": failed_tests,
        "recent_tests": tests,
    }


def get_lineage(
    table_fqn: str,
    upstream_depth: int = DEFAULT_LINEAGE_DEPTH,
    downstream_depth: int = DEFAULT_LINEAGE_DEPTH,
):
    table_fqn = str(table_fqn or "").strip()
    if not table_fqn:
        raise ValueError("table_fqn is required.")

    encoded_table_fqn = quote(table_fqn, safe="")
    res = _request(
        "GET",
        f"/lineage/table/name/{encoded_table_fqn}",
        params={
            "upstreamDepth": _bounded_depth(upstream_depth),
            "downstreamDepth": _bounded_depth(downstream_depth),
        },
    )
    return res.json()
