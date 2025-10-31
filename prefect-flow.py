# prefect flow goes here
from prefect import flow, task, get_run_logger
import httpx
import boto3
import time

# ------- TASK 1 - POPULATE SQS QUEUE -------
@task(retries=3, retry_delay_seconds=10)
def scatter_messages():
    """
    Calls the scatter API to populate the SQS queue.
    """
    logger = get_run_logger()
    try:
        url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/tsh3ut"
        response = httpx.post(url, timeout=10.0)
        response.raise_for_status() # catches HTTP errors
        
        payload = response.json()        
        queue_url = payload["sqs_url"]

        logger.info(f"Queue successfully populated: {queue_url}")
        return queue_url
    except Exception as e:
        logger.error(f"Error populating SQS queue: {e}")
        raise

# ------- TASK 2 - MONITOR QUEUE, RECEIVE, AND DELETE MESSAGES -------
@task
def get_queue_attributes(queue_url):
    """
    Monitor queue by checking message counts.
    Returns a dictionary with count of visible, not visible, and delayed messages.
    """
    logger = get_run_logger()
    sqs = boto3.client("sqs", region_name="us-east-1")
    
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
        
        logger.info(f"Queue attributes: {counts}")
        return counts
    
    except Exception as e:
        logger.error(f"Error getting queue attributes: {e}")
        return {"visible": 0, "not_visible": 0, "delayed": 0}

def delete_message(sqs, queue_url, receipt_handle, logger):
    """
    Delete a message from the SQS queue after successful processing.
    """
    try:
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
    except Exception as e:
        logger.error(f"Error deleting message: {e}")

@task(retries=45, retry_delay_seconds=10)
def receive_and_delete_messages(queue_url, poll_interval=3, max_wait_cycles=12):
    """
    Continuously monitors queue and retrieves messages until fully empty.
    Handles null responses and deletes each message after storing.
    Returns list of (order_no, word) pairs.
    """
    logger = get_run_logger()
    sqs = boto3.client("sqs", region_name="us-east-1")
    collected = []
    empty_cycles = 0 # to confirm if queue is truly empty
    
    try:
        while True:
            # Check how many messages are in the queue
            counts = get_queue_attributes.fn(queue_url)
            total_messages = sum(counts.values())
            
            if total_messages == 0:
                empty_cycles += 1 # increment empty cycle count if no messages
                if empty_cycles >= max_wait_cycles: # Confirmed empty after several checks
                    logger.info("Queue confirmed empty. Exiting retrieval loop.")
                    break
                time.sleep(poll_interval)
                continue
            else:
                empty_cycles = 0  # reset if new messages are found

            logger.info(f"Messages available: {total_messages}. Retrieving next message...")
            
            # Try to receive a message
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MessageSystemAttributeNames=['All'],
                MessageAttributeNames=['All'],
                MaxNumberOfMessages=1,
                VisibilityTimeout=45,
                WaitTimeSeconds=5
            )
            # Handle case where no message is returned
            messages = response.get("Messages", [])
            if not messages:
                logger.info("No visible messages received this cycle.")
                time.sleep(poll_interval)
                continue

            # Process message
            msg = messages[0]
            attrs = msg.get("MessageAttributes", {})
            order_no = attrs.get("order_no", {}).get("StringValue")
            word = attrs.get("word", {}).get("StringValue")

            if order_no and word:
                collected.append((int(order_no), word))
                logger.info(f"Received message {order_no}: {word}")
                delete_message(sqs, queue_url, msg["ReceiptHandle"], logger)
            else:
                logger.warning("Error occurred, message skipped.")

            time.sleep(1.5)

        logger.info(f"Retrieved and deleted {len(collected)} total messages.")
        return collected

    except Exception as e:
        logger.error(f"Error in message retrieval: {e}")
        raise

# ------- TASK 3 - ASSEMBLE COMPLETE QUOTES AND SUBMIT TO SQS -------
@task
def reassemble_phrase(collected_messages):
    """
    Sorts collected (order_no, word) pairs and reassembles into one phrase.
    """
    logger = get_run_logger()
    try:
        sorted_msgs = sorted(collected_messages, key=lambda x: x[0]) # sorts numerically by order_no
        phrase = " ".join([word for _, word in sorted_msgs]) # joins words in order with spaces into phrase
        logger.info(f"Reassembled phrase: {phrase}")
        return phrase
    except Exception as e:
        logger.error(f"Error reassembling phrase: {e}")
        raise

@task
def send_solution(uvaid, phrase, platform):
    """
    Send reassembled phrase to the dp2-submit SQS Queue.
    """
    logger = get_run_logger()
    sqs = boto3.client("sqs", region_name="us-east-1")
    url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
    message = "submission"

    try:
        response = sqs.send_message(
            QueueUrl=url,
            MessageBody=message,
            MessageAttributes={
                'uvaid': {
                    'DataType': 'String', 
                    'StringValue': uvaid
                    },
                'phrase': {
                    'DataType': 'String', 
                    'StringValue': phrase
                },
                'platform': {
                    'DataType': 'String', 
                    'StringValue': platform
                }
            }
        )
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
        if status == 200:
            logger.info("Successfully submitted solution (HTTP 200).")
        else:
            logger.warning(f"Submission returned HTTP {status}.")
        return status

    except Exception as e:
        logger.error(f"Error sending solution: {e}")
        raise

# PREFECT FLOW
@flow(name="quote_assembler")
def dp2_pipeline():
    """
    Prefect flow combining all tasks:
    1. Populates SQS queue.
    2. Monitors queue, receives, and deletes messages.
    3. Reassembles final phrase.
    4. Submits final phrase to submission SQS queue.
    """
    logger = get_run_logger()
    try:
        logger.info("Starting Quote Assembly Pipeline...")

        # Task 1
        queue_url = scatter_messages()

        # Task 2
        get_queue_attributes(queue_url)
        collected = receive_and_delete_messages(queue_url)

        # Task 3
        phrase = reassemble_phrase(collected)
        send_solution("tsh3ut", phrase, "prefect")

        logger.info("Quote Assembly Pipeline Complete.")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    dp2_pipeline()