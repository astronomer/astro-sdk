import json
import logging
import os

from dateutil.parser import parse
from flask import Flask, request

APP = Flask(__name__)
logger = logging.getLogger(__name__)

CWD = os.path.dirname(os.path.realpath(__file__))
DUMPS_DIR = os.path.join(CWD, "dumps")
os.makedirs(DUMPS_DIR, exist_ok=True)


@APP.route("/api/v1/lineage", methods=["POST"])
def dump():
    """Endpoint to dump lineage event to a local file system directory."""
    event_name = "default"
    try:
        js = json.loads(request.data)
        content = json.dumps(js, sort_keys=True, indent=4)

        date = parse(js["eventTime"]).date()
        job_name = js["job"]["name"]
        event_name = f"{date}-{job_name}"
    except TypeError:
        content = str(request.data, "UTF-8")
    file_path = f"{DUMPS_DIR}/{event_name}.json"

    logger.info("Written event %s to file %s", event_name, file_path)
    with open(file_path, "a") as f:
        f.write(content)
    return "", 200
