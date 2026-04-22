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

    try:
        backend = subprocess.Popen(backend_cmd, cwd=ROOT_DIR)
        time.sleep(2)  # Give backend time to start

        frontend = subprocess.Popen(frontend_cmd, cwd=ROOT_DIR)

        while True:
            # If Streamlit dies there's nothing left to show — stop everything.
            if frontend.poll() is not None:
                print("\nStreamlit exited. Shutting down...")
                backend.terminate()
                backend.wait(timeout=5)
                break

            # If the backend dies, restart it without touching Streamlit.
            if backend.poll() is not None:
                print("\nBackend exited unexpectedly — restarting...")
                backend = subprocess.Popen(backend_cmd, cwd=ROOT_DIR)
                time.sleep(2)

            time.sleep(1)

    except KeyboardInterrupt:
        print("\nShutting down...")
        for p in (backend, frontend):
            try:
                p.terminate()
                p.wait(timeout=5)
            except Exception:
                pass
        print("Stopped.")


if __name__ == "__main__":
    main()
