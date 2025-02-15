import requests
import time





ORTHANC_URL = "http://localhost:8042"
CLOUD_URL = "https://pacs.reportingbot.in"
ORTHANC_AUTH = ("admin", "phP@123!")  # Replace with actual credentials
CLOUD_AUTH = ("admin", "phP@123!")  # Replace with actual credentials

def fetch_failed_jobs():
    """Fetch failed jobs from Orthanc."""
    try:
        # Force Orthanc to refresh job list
        requests.post(f"{ORTHANC_URL}/jobs/reconstruct", auth=ORTHANC_AUTH)

        # Get the list of job IDs
        response = requests.get(f"{ORTHANC_URL}/jobs", auth=ORTHANC_AUTH)
        response.raise_for_status()
        job_ids = response.json()

        failed_jobs = []

        for job_id in job_ids:
            job_response = requests.get(f"{ORTHANC_URL}/jobs/{job_id}", auth=ORTHANC_AUTH)
            job_response.raise_for_status()
            job_details = job_response.json()

            if job_details.get("State") == "Failure":
                failed_jobs.append(job_id)

        return failed_jobs

    except requests.RequestException as e:
        return []

def retry_failed_jobs():
    """Retry failed jobs by resending their DICOM instances."""
    failed_jobs = fetch_failed_jobs()
    
    if not failed_jobs:
        return

    retried_jobs = []

    for job_id in failed_jobs:
        try:
            job_response = requests.get(f"{ORTHANC_URL}/jobs/{job_id}", auth=ORTHANC_AUTH)
            job_response.raise_for_status()
            job_details = job_response.json()

            dicom_id = job_details.get("Content", {}).get("ParentResources", [None])[0]
            if not dicom_id:
                continue

            dicom_response = requests.get(f"{ORTHANC_URL}/instances/{dicom_id}/file", auth=ORTHANC_AUTH)
            if dicom_response.status_code == 200:
                files = {"file": ("dicom.dcm", dicom_response.content)}
                cloud_response = requests.post(f"{CLOUD_URL}/instances", files=files, auth=CLOUD_AUTH)

                if cloud_response.status_code == 200:
                    retried_jobs.append(job_id)
                    # Delete old failed job after successful retry
                    requests.delete(f"{ORTHANC_URL}/jobs/{job_id}", auth=ORTHANC_AUTH)

        except requests.RequestException as e:
            pass

def main():
    """Main loop to check and retry failed jobs every 5 minutes."""
    while True:
        retry_failed_jobs()
        time.sleep(300)  # Wait 5 minutes before next check

if __name__ == "__main__":
    main()