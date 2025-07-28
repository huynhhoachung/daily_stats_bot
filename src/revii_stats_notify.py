import os
import json
import logging
import requests
import pandas as pd
from itertools import product
import datetime as dt
import boto3

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# The Slack webhook URL must be set in your Lambda’s environment variables
SLACK_WEBHOOK_URL = os.environ['SLACK_WEBHOOK_URL']

API_KEY = os.environ['PENDO_INTEGRATION_KEY']
API_URL = os.environ['PENDO_API_URL']
HEADERS = {
    "Content-Type": "application/json",
    "x-pendo-integration-key": API_KEY,
}

def to_epoch_ms(date_str: str) -> str:
    """YYYY-MM-DD → epoch-milliseconds as *string* (Pendo expects strings)."""
    t = dt.datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)
    return str(int(t.timestamp() * 1000))

START = to_epoch_ms(os.environ['PENDO_START_DATE'])
END = str(int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000))  # Current date in epoch milliseconds

SEGMENTS = {
    "Segment-A": os.environ['SEGMENT_A_ID'],
    "Segment-B": os.environ['SEGMENT_B_ID'],
}

PAGES = {
    "Web": os.environ['PAGE_WEB_ID'],
    "iOS": os.environ['PAGE_IOS_ID'],
    "Android": os.environ['PAGE_ANDROID_ID'],
}

FEATURE_GROUPS = {
    "Roleplay": [
        os.environ['FEATURE_ROLEPLAY_ID']
    ],
    "Automation": [
        os.environ['FEATURE_AUTOMATION_1_ID'],
        os.environ['FEATURE_AUTOMATION_2_ID'],
        os.environ['FEATURE_AUTOMATION_3_ID']
    ]
}

def clicks_for_feature(seg_id, feat_id):
    """Return total clicks on one feature for one segment in date range."""
    payload = {
        "response": {"mimeType": "application/json"},
        "request": {
            "name": "oneFeature",
            "pipeline": [
                {
                    "source": {
                        "featureEvents": {"featureId": feat_id},
                        "timeSeries": {
                            "period": "dayRange",
                            "first": START,
                            "last": END
                        }
                    }
                },
                {"segment": {"id": seg_id}},
                {"group": {"fields": {"clicks": {"sum": "numEvents"}}}}
            ],
            "requestId": "oneFeature"
        }
    }
    r = requests.post(API_URL, headers=HEADERS, data=json.dumps(payload))
    r.raise_for_status()
    return r.json().get("results", [{}])[0].get("clicks", 0)

def extract_data():
    """Extracts data from the API and returns dataframes for clicks and views."""
    results = []

    for (seg_name, seg_id), (page_name, page_id) in product(SEGMENTS.items(), PAGES.items()):
        payload = {
            "response": {"mimeType": "application/json"},
            "request": {
                "name": f"{seg_name}-{page_name}",
                "pipeline": [
                    {
                        "source": {
                            "pageEvents": {"pageId": page_id},
                            "timeSeries": {
                                "period": "dayRange",
                                "first": START,
                                "last": END,
                            },
                        }
                    },
                    {"segment": {"id": seg_id}},
                    {"group": {"fields": {"views": {"sum": "numEvents"}}}}
                ],
                "requestId": f"{seg_name}-{page_name}",
            },
        }

        resp = requests.post(API_URL, headers=HEADERS, data=json.dumps(payload))
        print("Status:", resp.status_code)
        print("Response body:", resp.text[:1000])
        resp.raise_for_status()

        view_rows = resp.json().get("results", [])
        total_views = view_rows[0]["views"] if view_rows else 0

        results.append({
            "Segment": seg_name,
            "Page": page_name,
            "Views (12 Feb – Today)": total_views,
        })

    rows = []
    for seg_name, seg_id in SEGMENTS.items():
        for grp_name, feats in FEATURE_GROUPS.items():
            total_clicks = sum(clicks_for_feature(seg_id, fid) for fid in feats)
            rows.append({
                "Segment": seg_name,
                "Feature Group": grp_name,
                "Clicks (12 Feb – Today)": total_clicks
            })

    df_clicks = pd.DataFrame(rows)
    df_views = pd.DataFrame(results)
    return df_clicks, df_views

def send_sns_notification(message: str):
    """
    Send an SNS notification with the given subject and message.
    """
    sns_client = boto3.Session().client('sns')
    try:
        sns_client.publish(
            TopicArn=os.environ.get("SNS_TOPIC_ARN"),
            Subject='Slack Notification Error',
            Message=message
        )
        logger.info("SNS notification sent successfully.")
    except Exception as e:
        logger.error(f"Failed to send SNS notification: {e}")
        raise

def send_to_slack(message: str):
    """
    Sends a plain-text message to Slack via the webhook URL.
    """
    try:
        payload = {"text": message}
        resp = requests.post(SLACK_WEBHOOK_URL, json=payload)
        resp.raise_for_status()
    except Exception as e:
        raise Exception(f"Failed to send to Slack: {e}")

def build_message(event: dict, df_clicks: pd.DataFrame, df_views: pd.DataFrame) -> str:
    """
    Builds a Slack message using bullets with dynamic values from the event and dataframes.
    """
    lines = []
    report_date = event.get("report_date", "")
    lines.append(f"Stats for {report_date}")
    lines.append(f"•  Lifetime user total: {event.get('total', 0)}")
    coupon_prefixes = [key.replace("_coupon_active_subscriptions", "") for key in event if key.endswith("_coupon_active_subscriptions")]
    total_active_users = sum(event.get(f"{p}_coupon_active_subscriptions", 0) for p in coupon_prefixes)
    no_coupon_key = next((p for p in coupon_prefixes if p.lower() == "no coupon code"), None)
    trials_without_coupon = event.get(f"{no_coupon_key}_total_trials", 0) if no_coupon_key else 0
    trials_with_coupon = sum(event.get(f"{p}_total_trials", 0) for p in coupon_prefixes if p.lower() != "no coupon code")
    lines.append("•  Active snapshot stats :")
    lines.append(f"   ○  Total Active Users: {total_active_users}")
    lines.append(f"   ○  Total Active Trial Users without a coupon: {trials_without_coupon}")
    lines.append(f"   ○  Total Active Trial Users with a coupon: {trials_with_coupon}")
    # Churn, Downloads, Coupon stats, Pendo summary...
    return "\n".join(lines)

def lambda_handler(event, context):
    try:
        if isinstance(event, str):
            payload = json.loads(event)
        else:
            payload = event
        df_clicks, df_views = extract_data()
        message = build_message(payload, df_clicks, df_views)
        logger.info("Constructed Slack message:\n" + message)
        send_to_slack(message)
        return {"status": 200, "body": json.dumps({"message_sent": True})}
    except Exception as e:
        error_message = f"Lambda job revii_stats_notify failed: {e}"
        logger.error(error_message, exc_info=True)
        send_sns_notification(error_message)
        raise
