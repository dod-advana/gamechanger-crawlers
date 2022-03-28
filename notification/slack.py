import os
import requests
import logging

logger = logging.getLogger()


def send_notification(
    message: str, SLACK_HOOK_CHANNEL=None, SLACK_HOOK_URL=None, use_env_vars=True
):
    headers = {"Content-Type": "application/json"}

    if use_env_vars:
        should_send = os.environ.get("SEND_NOTIFICATIONS")
        channel = os.environ.get("SLACK_HOOK_CHANNEL")

        if should_send:
            url = os.environ.get("SLACK_HOOK_URL")

            res = requests.post(
                url, data={"channel": channel, "text": message}, headers=headers
            )

            logger.info(f"Status code: {res.status_code}")
            logger.debug(f"Response text: {res.text}")
        else:
            logger.error("SEND_NOTIFICATIONS env not set, did not send:\n")

    elif SLACK_HOOK_URL:
        res = requests.post(
            SLACK_HOOK_URL,
            data={"channel": SLACK_HOOK_CHANNEL, "text": message},
            headers=headers,
        )
        logger.info(f"Status code: {res.status_code}")
        logger.debug(f"Response text: {res.text}")
