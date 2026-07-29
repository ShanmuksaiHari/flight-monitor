import json
import os
import requests
from datetime import datetime
from dotenv import load_dotenv

# Load secrets from .env file
load_dotenv()

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
INCIDENTS_FILE = "incidents.jsonl"


def send_slack_alert(alarm):
    """
    Send a Slack message to #flight-alerts when an alarm fires.
    Never crashes the pipeline if Slack is unreachable.
    """
    if not SLACK_WEBHOOK_URL:
        print("⚠️  No Slack webhook configured — skipping alert")
        return False

    severity = alarm.get("severity", "warning")
    alarm_type = alarm.get("alarm_type", "UNKNOWN")
    message = alarm.get("message", "Unknown alarm")

    # Choose emoji based on severity
    emoji = "🔴" if severity == "critical" else "🟡"

    slack_message = {
        "text": f"{emoji} *FLIGHT MONITOR ALARM*",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"{emoji} *FLIGHT MONITOR — {alarm_type} ALARM*\n{message}"
                }
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"🕐 {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC | Severity: {severity.upper()}"
                    }
                ]
            }
        ]
    }

    try:
        response = requests.post(
            SLACK_WEBHOOK_URL,
            json=slack_message,
            timeout=5
        )
        if response.status_code == 200:
            print(f"📱 Slack alert sent: {alarm_type}")
            return True
        else:
            print(f"⚠️  Slack returned {response.status_code}")
            return False
    except Exception as e:
        # Never crash the pipeline for a Slack failure
        print(f"⚠️  Slack alert failed (pipeline continues): {e}")
        return False


def log_incident(alarm):
    """
    Append alarm to incidents.jsonl log file.
    This file powers the incident history in the dashboard.
    """
    incident = {
        "timestamp": datetime.utcnow().isoformat(),
        "alarm_type": alarm.get("alarm_type", "UNKNOWN"),
        "severity": alarm.get("severity", "warning"),
        "message": alarm.get("message", ""),
        "status": "fired"
    }

    try:
        with open(INCIDENTS_FILE, "a") as f:
            f.write(json.dumps(incident) + "\n")
        print(f"📝 Incident logged: {incident['alarm_type']}")
    except Exception as e:
        print(f"⚠️  Failed to log incident: {e}")


def handle_alarm(alarm):
    """
    Handle a fired alarm:
    1. Log it to incidents.jsonl
    2. Send Slack alert
    """
    print(f"\n🚨 ALARM FIRED: {alarm.get('alarm_type')} — {alarm.get('message')}")
    log_incident(alarm)
    send_slack_alert(alarm)


def handle_alarms(alarm_results):
    """
    Handle a list of alarms from the detector.
    """
    for alarm in alarm_results:
        if alarm.get("alarm"):
            handle_alarm(alarm)


if __name__ == "__main__":
    # Test — send a real Slack message
    print("Testing Slack alert...")
    test_alarm = {
        "alarm": True,
        "alarm_type": "TEST",
        "severity": "warning",
        "message": "This is a test alarm from your Flight Monitor pipeline — everything is working!"
    }
    handle_alarm(test_alarm)
    print("\n✅ Check your #flight-alerts Slack channel!")