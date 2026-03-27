from prefect import flow, task
import datetime

@task(log_prints=True)
def check_system():
    print(f"--- Kiểm tra hệ thống lúc: {datetime.datetime.now()} ---")
    return "Ready"

@flow(name="Test", log_prints=True)
def test_flow():
    status = check_system()
    print(f"Trạng thái worker: {status}")
    print("--- OK ---")

if __name__ == "__main__":
    test_flow.serve(
        name="docker-test-deployment"
    )