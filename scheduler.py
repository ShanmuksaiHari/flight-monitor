import schedule
import time
import subprocess

def run_dbt():
    print("⏰ Running dbt...")
    result = subprocess.run(
        ["dbt", "run"],
        cwd="./dbt_project/flight_monitor",
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode == 0:
        print("✅ dbt run complete")
    else:
        print(f"❌ dbt failed: {result.stderr}")

run_dbt()
schedule.every(20).minutes.do(run_dbt)
print("📅 Scheduler running — dbt every 20 minutes")
while True:
    schedule.run_pending()
    time.sleep(1)