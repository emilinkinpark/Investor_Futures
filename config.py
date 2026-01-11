# config.py
import os
from dotenv import load_dotenv
import importlib
import subprocess
import sys

# Load environment variables from .env file
load_dotenv()

API_KEY = os.getenv('BINANCE_API_KEY')
API_SECRET = os.getenv('BINANCE_API_SECRET')
BASE_URL = os.getenv('BINANCE_BASE_URL', 'https://fapi.binance.com')

REQUIRED_PACKAGES = {
    # module_name: pip_name
    "dotenv": "python-dotenv",
    "websocket": "websocket-client",
    "requests": "requests",
    "pandas": "pandas",
    "numpy": "numpy",
    "matplotlib": "matplotlib",  # used in marketpredictor_tab.py
}


def ensure_packages():
    """
    Try to import each required module; if missing, install it with pip
    into the *current* Python environment, then restart this process once.
    """
    missing = []

    for module_name, pip_name in REQUIRED_PACKAGES.items():
        try:
            importlib.import_module(module_name)
        except ImportError:
            missing.append((module_name, pip_name))

    if not missing:
        return  # everything is available

    print("Missing packages detected:")
    for module_name, pip_name in missing:
        print(f"  - {module_name} (installing: {pip_name})")

    # Install each missing package via pip
    for module_name, pip_name in missing:
        try:
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", pip_name]
            )
        except Exception as e:
            print(f"Failed to install {pip_name}: {e}")

    # After installations, restart the script once so imports see new modules
    print("Restarting process to pick up newly installed packages...")
    os.execv(sys.executable, [sys.executable] + sys.argv)


# Run the check before importing the rest of the app
import os  # needed for os.execv above
ensure_packages()