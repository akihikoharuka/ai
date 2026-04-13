"""Start both FastAPI backend and Streamlit frontend."""

import subprocess
import sys
import os
import signal
import time

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


def main():
    # Start FastAPI backend
    backend_cmd = [
        sys.executable, "-m", "uvicorn",
        "backend.main:app",
        "--host", "0.0.0.0",
        "--port", "8000",
        "--reload",
    ]

    # Start Streamlit frontend
    frontend_cmd = [
        sys.executable, "-m", "streamlit",
        "run", "frontend/app.py",
        "--server.port", "8501",
        "--server.headless", "true",
    ]

    print("Starting Synthetic Data Generator...")
    print("Backend:  http://localhost:8000")
    print("Frontend: http://localhost:8501")
    print("Press Ctrl+C to stop both services.\n")

    procs = []
    try:
        backend = subprocess.Popen(backend_cmd, cwd=ROOT_DIR)
        procs.append(backend)
        time.sleep(2)  # Give backend time to start

        frontend = subprocess.Popen(frontend_cmd, cwd=ROOT_DIR)
        procs.append(frontend)

        # Wait for either process to exit
        while True:
            for p in procs:
                if p.poll() is not None:
                    raise KeyboardInterrupt
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nShutting down...")
        for p in procs:
            p.terminate()
        for p in procs:
            p.wait(timeout=5)
        print("Stopped.")


if __name__ == "__main__":
    main()
