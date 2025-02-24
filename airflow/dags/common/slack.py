from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.models import DAG, Variable

SLACK_CONFIG = {
    "info": {"color": "", "emoji": ":information_source:"},
    "success": {"color": "#36a64f", "emoji": ":white_check_mark:"},
    "warn": {"color": "#ffcc00", "emoji": ":warning:"},
    "error": {"color": "#ff0000", "emoji": ":x:"},
}


def slack_info_message(
    message: str, dag: DAG, task_id: str, trigger_rule: str = "all_success"
):
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
        trigger_rule=trigger_rule,
        slack_webhook_conn_id="slack_default",
        message=f"*INFO*: {message}",
        attachments=attachments,
    )


def slack_warning_message(message: str, dag: DAG, task_id: str):
    AIRFLOW_UI_URL = Variable.get("AIRFLOW_UI_URL")
    attachments = [
        {
            "color": "#ffcc00",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f":warning: *WARNING* {message}\n",
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
            ],
        }
    ]
    return SlackWebhookOperator(
        task_id=f"{task_id}",
        slack_webhook_conn_id="slack_default",
        message=f"*WARNING*: {message}",
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

    message = f"*Task* `{task_id}` failed."

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
                            "text": f"*Reason:*\n```{exception}```",
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
