# airflow DAG goes here
"""
Airflow DAG version of the DP2 Prefect Flow.
Implements the same pipeline:
1. Populate SQS queue.
2. Monitor queue and retrieve/delete messages.
3. Reassemble final phrase.
4. Submit the phrase to dp2-submit queue.
"""

from airflow.decorators import dag, task
from airflow.utils.log.logging_mixin import LoggingMixin
from pendulum import datetime
from datetime import timedelta
import httpx
import time
from airflow.models import Variable

AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")

import boto3
sqs = boto3.client(
    "sqs",
    region_name="us-east-1",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# Helper function to get queue attributes
def get_queue_attributes(queue_url):
    """
    Returns current visible, not visible, and delayed message counts.
    """
    log = LoggingMixin().log
    try:
        response = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
                "ApproximateNumberOfMessagesDelayed"
            ]
        )
        attrs = response.get("Attributes", {})
        counts = {
            "visible": int(attrs.get("ApproximateNumberOfMessages", 0)),
            "not_visible": int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0)),
            "delayed": int(attrs.get("ApproximateNumberOfMessagesDelayed", 0))
        }
        log.info(f"Queue attributes: {counts}")
        return counts
    except Exception as e:
        log.error(f"Error getting queue attributes: {e}")
        return {"visible": 0, "not_visible": 0, "delayed": 0}
    
# Helper function to delete messages
def delete_message(sqs, queue_url, receipt_handle):
    """Deletes a message from the SQS queue."""
    log = LoggingMixin().log
    try:
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
    except Exception as e:
        log.error(f"Error deleting message: {e}")

# ------- TASK 1: POPULATE SQS QUEUE -------
@task(retries=3, retry_delay=timedelta(seconds=10))
def scatter_messages():
    """
    Calls the scatter API to populate the SQS queue.
    """
    log = LoggingMixin().log
    url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/tsh3ut"
    try:
        response = httpx.post(url, timeout=10.0)
        response.raise_for_status()
        payload = response.json()      
        queue_url = payload["sqs_url"]
        log.info(f"Queue successfully populated: {queue_url}")
        return queue_url
    except Exception as e:
        log.error(f"Error populating SQS queue: {e}")
        raise

# ------- TASK 2 - MONITOR QUEUE, RECEIVE, AND DELETE MESSAGES -------
@task(retries=45, retry_delay=timedelta(seconds=10))
def receive_and_delete_messages(queue_url, poll_interval=3, max_wait_cycles=12):
    """
    Continuously monitors the queue and retrieves/deletes messages until empty.
    Returns list of (order_no, word) pairs.
    """
    log = LoggingMixin().log
    collected = []
    empty_cycles = 0

    try:
        while True:
            counts = get_queue_attributes(queue_url)
            total_messages = sum(counts.values())

            if total_messages == 0:
                empty_cycles += 1
                if empty_cycles >= max_wait_cycles:
                    log.info("Queue confirmed empty. Exiting retrieval loop.")
                    break
                time.sleep(poll_interval)
                continue
            else:
                empty_cycles = 0

            response = sqs.receive_message(
                QueueUrl=queue_url,
                MessageSystemAttributeNames=['All'],
                MessageAttributeNames=['All'],
                MaxNumberOfMessages=1,
                VisibilityTimeout=45,
                WaitTimeSeconds=5
            )

            messages = response.get("Messages", [])
            if not messages:
                time.sleep(poll_interval)
                continue

            msg = messages[0]
            attrs = msg.get("MessageAttributes", {})
            order_no = attrs.get("order_no", {}).get("StringValue")
            word = attrs.get("word", {}).get("StringValue")

            if order_no and word:
                collected.append((int(order_no), word))
                log.info(f"Received message {order_no}: {word}")
                delete_message(sqs, queue_url, msg["ReceiptHandle"])
            else:
                log.warning("Malformed message skipped.")

            time.sleep(1.5)

        log.info(f"Retrieved and deleted {len(collected)} total messages.")
        return collected

    except Exception as e:
        log.error(f"Error in message retrieval: {e}")
        raise

# ------- TASK 3 - REASSEMBLE AND SUBMIT PHRASE -------
@task
def reassemble_phrase(collected_messages):
    """
    Sorts (order_no, word) pairs and reassembles into one phrase.
    """
    log = LoggingMixin().log
    try:
        sorted_msgs = sorted(collected_messages, key=lambda x: x[0])
        phrase = " ".join([word for _, word in sorted_msgs])
        log.info(f"Reassembled phrase: {phrase}")
        return phrase
    except Exception as e:
        log.error(f"Error reassembling phrase: {e}")
        raise

@task
def send_solution(uvaid, phrase, platform):
    """
    Send reassembled phrase to the dp2-submit SQS Queue.
    """
    log = LoggingMixin().log
    url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
    try:
        response = sqs.send_message(
            QueueUrl=url,
            MessageBody="submission",
            MessageAttributes={
                'uvaid': {'DataType': 'String', 'StringValue': uvaid},
                'phrase': {'DataType': 'String', 'StringValue': phrase},
                'platform': {'DataType': 'String', 'StringValue': platform}
            }
        )
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
        if status == 200:
            log.info("Successfully submitted solution (HTTP 200).")
        else:
            log.warning(f"Submission returned HTTP {status}.")
        return status
    except Exception as e:
        log.error(f"Error sending solution: {e}")
        raise

# ------- DAG DEFINITION -------
@dag(
    dag_id="dp2_airflow_dag",
    description="DS3022 Data Project 2 — Airflow DAG version of the Quote Assembler pipeline.",
    start_date=datetime(2025, 10, 28),
    schedule=None,
    catchup=False,
)
def dp2_airflow_dag():
    """
    Airflow DAG equivalent of the Prefect pipeline.
    """
    queue_url = scatter_messages()
    collected = receive_and_delete_messages(queue_url)
    phrase = reassemble_phrase(collected)
    send_solution("tsh3ut", phrase, "airflow")

dp2_airflow_dag()