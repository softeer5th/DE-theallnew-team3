from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.models import DAG, Variable

SLACK_CONFIG = {
    "info": {"color": "", "emoji": ":information_source:"},
    "success": {"color": "#36a64f", "emoji": ":white_check_mark:"},
    "warn": {"color": "#ffcc00", "emoji": ":warning:"},
    "error": {"color": "#ff0000", "emoji": ":x:"},
}


def slack_info_message(message: str, dag: DAG, task_id: str):
    AIRFLOW_UI_URL = Variable.get("AIRFLOW_UI_URL")
    attachments = [
        {
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f":information_source: *INFO* {message}\n",
                    },
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Dag ID*: `{dag.dag_id}`",
                    },
                },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {
                                "type": "plain_text",
                                "text": ":globe_with_meridians: Open DAG in Airflow UI",
                                "emoji": True,
                            },
                            "url": f"{AIRFLOW_UI_URL}/dags/{dag.dag_id}/grid",
                        }
                    ],
                },
            ]
        }
    ]
    return SlackWebhookOperator(
        task_id=f"slack_info_{task_id}",
        slack_webhook_conn_id="slack_default",
        message=f"*INFO*: {message}",
        attachments=attachments,
    )


def slack_handle_task_failure(context):
    AIRFLOW_UI_URL = Variable.get("AIRFLOW_UI_URL")

    dag_id = context["dag"].dag_id
    task_id = context["ti"].task_id
    run_id = context["ti"].run_id
    ds = context["ds"]
    exception = context["exception"]
    log_url = context["ti"].log_url

    print(context["ti"])
    print(context["ti"].log_url)

    message = f"*Task* `{task_id}` failed with `{exception}`."

    attachments = [
        {
            "color": "#ff0000",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f":x: *ERROR* {message}\n",
                    },
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Dag ID:*\n`{dag_id}`"},
                        {"type": "mrkdwn", "text": f"*Task ID:*\n`{task_id}`"},
                        {
                            "type": "mrkdwn",
                            "text": f"*Run ID:*\n`{run_id}`",
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Date:*\n`{ds}`",
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Reason:*\n`{exception}`",
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Log URL:*\n<{log_url}|:page_facing_up: View Log>",
                        },
                    ],
                },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {
                                "type": "plain_text",
                                "text": ":globe_with_meridians: Open in Airflow UI",
                                "emoji": True,
                            },
                            "url": f"{AIRFLOW_UI_URL}/dags/{dag_id}/grid",
                        },
                    ],
                },
            ],
        }
    ]
    hook = SlackWebhookHook(slack_webhook_conn_id="slack_default")
    hook.send(text=message, attachments=attachments)


# def send_slack_notification(
#     message="",
#     attachments=None,
#     channel=None,
# ):
#     """Send message to Slack.
#     message: Text of the message to send. See below for an explanation of
#     formatting. This field is usually required, unless you're providing only
#     attachments instead. Provide no more than 40,000 characters or risk truncati
#     attachments: [list of max 100] dict[s] slack message attachment[s]
#     see https://api.slack.com/docs/message-attachments#attachment_structure
#     channel:  a channel in your workspace starting with `#`
#     """
#     assert isinstance(message, str) and message or attachments
#     if isinstance(attachments, dict):
#         attachments = [attachments]

#     notification_operator = SlackWebhookOperator(
#         task_id="slack_failed", message=message, attachments=attachments
#     )
#     return notification_operator.execute(attachments)


# def failed_task_slack_notification(kwargs):
#     """Send failed task notification with context provided by operator."""
#     print(kwargs["ti"])
#     dag = kwargs["ti"].dag_id
#     run_id = kwargs["run_id"]
#     task = kwargs["ti"].task_id
#     exec_date = kwargs["execution_date"]
#     try_number = kwargs["ti"].try_number - 1
#     max_tries = kwargs["ti"].max_tries + 1
#     exception = kwargs["exception"]
#     log_url = kwargs["ti"].log_url
#     # command = kwargs['ti'].command(),

#     message = (
#         f"`DAG`  {dag}"
#         f"\n`Run Id`  {run_id}"
#         f"\n`Task`  {task} _(try {try_number} of {max_tries})_"
#         f"\n`Execution`  {exec_date}"
#         f"\n```{exception}```"
#         # f'`Command`  {command}\n'
#     )

#     attachments = {
#         "mrkdwn_in": ["text", "pretext"],
#         "pretext": ":red_circle: *Task Failed*",
#         "title": "airflow-web.wheely-dev.com",
#         "title_link": f"https://airflow-web.wheely-dev.com",
#         "text": message,
#         "actions": [
#             {
#                 "type": "button",
#                 "name": "view log",
#                 "text": "View log :airflow:",
#                 "url": log_url,
#                 "style": "primary",
#             },
#         ],
#         "color": "danger",  # 'good', 'warning', 'danger', or hex ('#439FE0')
#         "fallback": "details",  # Required plain-text summary of the attachment
#     }

#     send_slack_notification(attachments=attachments)
