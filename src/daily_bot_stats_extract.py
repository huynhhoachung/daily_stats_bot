import boto3
import json
import psycopg2
from datetime import datetime
from pytz import timezone
import os
from decimal import Decimal
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create a session for boto3
session = boto3.Session()

pst_timezone = timezone('PST8PDT')
current_date_pst = datetime.now(pst_timezone).strftime('%m/%d/%Y')
lambda_client = boto3.client('lambda')

def get_data_from_redshift():
    """
    Retrieve daily metrics, churn stats, coupon stats, and signup-by-coupon stats from Redshift.
    """
    try:
        conn = psycopg2.connect(
            dbname=os.environ['DBNAME'],
            user=os.environ['USER'],
            password=os.environ['PASSWORD'],
            host=os.environ['HOST'],
            port=os.environ['PORT']
        )
        cursor = conn.cursor()
    except Exception as e:
        raise RuntimeError(f"Error connecting to Redshift: {e}")

    result = {}

    # 1) Daily daily_bot_stats_daily metrics
    static_metrics_query = """
    SELECT
        TO_CHAR(CURRENT_DATE - INTERVAL '1 day', 'MM/DD/YYYY') AS report_date,
        rsd.total,
        rsd.trials,
        rsd.daily_bot2025_trials,
        rsd.revenue30_trials,
        rsd.active_subscriptions,
        rsd.past_due_subscriptions,
        rsd.yearly_subscriptions,
        rsd.cancelled_subscriptions,
        rsd.cancel_after_trial,
        rsd.cancel_during_trial,
        rsd.ios_first_time_downloads,
        rsd.ios_total_downloads,
        rsd.google_play_active_installs
    FROM daily_bot_stats_daily AS rsd;
    """
    cursor.execute(static_metrics_query)
    row = cursor.fetchone()
    cols = [desc[0] for desc in cursor.description]
    result.update(dict(zip(cols, row)))

    # 2) Churn signup stats split by period
    churn_query = """
    WITH
    churn_total AS (
      SELECT
        'all_time'::VARCHAR AS period,
        COUNT(DISTINCT CASE WHEN status = 'Conversion' THEN email END)   AS churn_conversion_all,
        COUNT(DISTINCT CASE WHEN status = 'Canceled'   THEN email END)   AS churn_canceled_all,
        COUNT(DISTINCT CASE WHEN status = 'None'       THEN email END)   AS churn_none_all,
        COUNT(DISTINCT CASE WHEN status = 'New Signup' THEN email END)   AS churn_new_signup_all,
        COUNT(DISTINCT CASE WHEN status_date >= CURRENT_DATE - INTERVAL '7 days' THEN email END)   AS churn_past_due_all
      FROM daily_bot_signup_churn
      WHERE email IS NOT NULL
    ),
    churn_last_week AS (
      SELECT
        'last_week'::VARCHAR AS period,
        COUNT(DISTINCT CASE WHEN status = 'Conversion' THEN email END)   AS churn_conversion_week,
        COUNT(DISTINCT CASE WHEN status = 'Canceled'   THEN email END)   AS churn_canceled_week,
        COUNT(DISTINCT CASE WHEN status = 'None'       THEN email END)   AS churn_none_week,
        COUNT(DISTINCT CASE WHEN status = 'New Signup' THEN email END)   AS churn_new_signup_week,
        COUNT(DISTINCT CASE WHEN status_date >= CURRENT_DATE - INTERVAL '7 days' THEN email END)   AS churn_past_due_week
      FROM daily_bot_signup_churn
      WHERE email IS NOT NULL
        AND status_date >= CURRENT_DATE - INTERVAL '7 days'
        AND status_date <  CURRENT_DATE
    ),
    churn_last_month AS (
      SELECT
        'last_month'::VARCHAR AS period,
        COUNT(DISTINCT CASE WHEN status = 'Conversion' THEN email END)   AS churn_conversion_month,
        COUNT(DISTINCT CASE WHEN status = 'Canceled'   THEN email END)   AS churn_canceled_month,
        COUNT(DISTINCT CASE WHEN status = 'None'       THEN email END)   AS churn_none_month,
        COUNT(DISTINCT CASE WHEN status = 'New Signup' THEN email END)   AS churn_new_signup_month,
        COUNT(DISTINCT CASE WHEN status_date >= CURRENT_DATE - INTERVAL '1 month' THEN email END)   AS churn_past_due_month
      FROM daily_bot_signup_churn
      WHERE email IS NOT NULL
        AND status_date >= CURRENT_DATE - INTERVAL '1 month'
        AND status_date <  CURRENT_DATE
    )
    SELECT
      ct.churn_conversion_all,
      ct.churn_canceled_all,
      ct.churn_none_all,
      ct.churn_new_signup_all,
      ct.churn_past_due_all,
      lw.churn_conversion_week,
      lw.churn_canceled_week,
      lw.churn_none_week,
      lw.churn_new_signup_week,
      lw.churn_past_due_week,
      lm.churn_conversion_month,
      lm.churn_canceled_month,
      lm.churn_none_month,
      lm.churn_new_signup_month,
      lm.churn_past_due_month
    FROM churn_total AS ct
    CROSS JOIN churn_last_week AS lw
    CROSS JOIN churn_last_month AS lm;
    """
    cursor.execute(churn_query)
    churn_row = cursor.fetchone()
    churn_cols = [desc[0] for desc in cursor.description]
    result.update(dict(zip(churn_cols, churn_row)))

    # 3) Coupon stats
    coupon_query = """
    WITH
        daily_bot_coupon_trial AS (
            SELECT coupon_code, COUNT(DISTINCT email) AS coupon_active_subscriptions
            FROM daily_bot_trial_coupon_activity
            WHERE email IS NOT NULL
            GROUP BY coupon_code
        ),
        all_time_conversion_rate_by_coupon AS (
            SELECT coupon_code, total_trials, total_conversions, percent_converted
            FROM daily_bot_all_time_conversion_rate_by_coupon
        ),
        base_data AS (
            SELECT
                rct.coupon_code,
                rct.coupon_active_subscriptions,
                atc.total_trials,
                atc.total_conversions,
                atc.percent_converted
            FROM daily_bot_coupon_trial AS rct
            JOIN all_time_conversion_rate_by_coupon AS atc
              ON rct.coupon_code = atc.coupon_code
        )
    SELECT coupon_code, coupon_active_subscriptions, total_trials, total_conversions, percent_converted
    FROM base_data;
    """
    cursor.execute(coupon_query)
    for row in cursor.fetchall():
        code, active, trials, convs, rate = row
        result[f"{code}_coupon_active_subscriptions"] = active
        result[f"{code}_total_trials"] = trials
        result[f"{code}_total_conversions"] = convs
        result[f"{code}_conversion_rate"] = float(rate) if isinstance(rate, Decimal) else rate

    # 4) Signups by coupon stats
    signup_query = """
    WITH
        t_yesterday AS (
          SELECT
            'all' AS period,
            SUM(CASE WHEN coupon_code IS NULL OR coupon_code = 'No Coupon Code' THEN 1 ELSE 0 END) AS new_signups_without_coupon,
            SUM(CASE WHEN coupon_code IS NOT NULL AND coupon_code <> 'No Coupon Code' THEN 1 ELSE 0 END) AS new_signups_with_coupon,
            COUNT(1) AS total_new_signups
          FROM daily_bot_trial_signups_by_coupon
          WHERE signup_date::date <= DATEADD(day, -1, CURRENT_DATE)
        ),
        t_last_week AS (
          SELECT
            'last_week' AS period,
            SUM(CASE WHEN coupon_code IS NULL OR coupon_code = 'No Coupon Code' THEN 1 ELSE 0 END) AS new_signups_without_coupon,
            SUM(CASE WHEN coupon_code IS NOT NULL AND coupon_code <> 'No Coupon Code' THEN 1 ELSE 0 END) AS new_signups_with_coupon,
            COUNT(1) AS total_new_signups
          FROM daily_bot_trial_signups_by_coupon
          WHERE signup_date::date >= DATEADD(day, -7, CURRENT_DATE)
            AND signup_date::date <  DATEADD(day, 0, CURRENT_DATE)
        ),
        t_last_month AS (
          SELECT
            'last_month' AS period,
            SUM(CASE WHEN coupon_code IS NULL OR coupon_code = 'No Coupon Code' THEN 1 ELSE 0 END) AS new_signups_without_coupon,
            SUM(CASE WHEN coupon_code IS NOT NULL AND coupon_code <> 'No Coupon Code' THEN 1 ELSE 0 END) AS new_signups_with_coupon,
            COUNT(1) AS total_new_signups
          FROM daily_bot_trial_signups_by_coupon
          WHERE signup_date::date >= DATEADD(month, -1, CURRENT_DATE)
            AND signup_date::date <  DATEADD(day, 0, CURRENT_DATE)
        )
    SELECT *
    FROM t_yesterday
    UNION ALL
    SELECT *
    FROM t_last_week
    UNION ALL
    SELECT *
    FROM t_last_month;
    """
    cursor.execute(signup_query)
    rows = cursor.fetchall()
    for period, without_coupon, with_coupon, total in rows:
        result[f"{period}_new_signups_without_coupon"] = without_coupon
        result[f"{period}_new_signups_with_coupon"] = with_coupon
        result[f"{period}_total_new_signups"] = total

    cursor.close()
    conn.close()
    return result

def send_sns_notification(message):
    sns_client = session.client('sns')
    try:
        sns_client.publish(
            TopicArn=os.environ['SNS_TOPIC_ARN'],
            Subject='Slack Notification',
            Message=f'Lambda job daily_bot_stats_extract failed: {message}'
        )
        logger.info("SNS notification sent successfully.")
    except Exception as e:
        logger.error(f"Failed to send SNS notification: {e}")
        raise

def lambda_handler(event, context):
    try:
        redshift_data = get_data_from_redshift()
        lambda_client.invoke(
            FunctionName='daily_bot-stats-notify',
            InvocationType='Event',
            Payload=json.dumps(redshift_data)
        )
        return {"status": "Payload sent to daily_bot_stats_notify"}
    except Exception as e:
        logger.error(f"Lambda daily_bot_stats_extract failed: {e}", exc_info=True)
        send_sns_notification(str(e))
        raise
