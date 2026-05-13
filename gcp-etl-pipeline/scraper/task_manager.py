import json
import datetime
import logging
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2

logger = logging.getLogger(__name__)


def create_scrape_tasks(
    project_id: str,
    region: str,
    queue_name: str,
    function_url: str,
    sa_email: str,
    pages: list[int],
    stagger_seconds: int = 2,
) -> list[str]:
    """
    Create one Cloud Task per page. Tasks are staggered to avoid
    hitting the scrape target with a burst of concurrent requests.
    """
    client = tasks_v2.CloudTasksClient()
    parent = client.queue_path(project_id, region, queue_name)

    task_names = []
    for i, page in enumerate(pages):
        payload = {
            "page": page,
            "source": "quotes.toscrape.com",
        }
        task = {
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": function_url,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps(payload).encode(),
                "oidc_token": {"service_account_email": sa_email},
            },
            "schedule_time": _future_timestamp(i * stagger_seconds),
        }

        response = client.create_task(request={"parent": parent, "task": task})
        task_names.append(response.name)
        logger.info("Created task for page %d: %s", page, response.name)

    return task_names


def _future_timestamp(seconds: int) -> timestamp_pb2.Timestamp:
    dt = datetime.datetime.utcnow() + datetime.timedelta(seconds=seconds)
    ts = timestamp_pb2.Timestamp()
    ts.FromDatetime(dt)
    return ts
