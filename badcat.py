#!/usr/bin/env python3
"""
Sonarr/Radarr Indexer Category Sync Tool
Optimized for Python 3.13+
"""

import os
import re
import sys
import json
import time
import signal
import logging
import threading
from pathlib import Path
from typing import Optional, Any

import requests
from signalrcore.hub_connection_builder import HubConnectionBuilder
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("badcat")

debounce: dict[str, float] = {}

def bouncy(inst_name: str) -> bool:
    now = time.time()
    if inst_name in debounce and now - debounce[inst_name] < 3.0:
        return True

    debounce[inst_name] = now
    return False

# --- Config Manager ---
class ConfigManager:
    @staticmethod
    def load_instances() -> list[dict[str, str]]:
        config_path = os.environ.get("INSTANCE_CONFIG_JSON")
        if not config_path:
            raise ValueError("INSTANCE_CONFIG_JSON env var required")

        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Config not found: {config_path}")

        with open(path) as f:
            data = json.load(f)

        for k, v in data.items():
            v["name"] = k

        return data

    @staticmethod
    def get_output_folder() -> Path:
        folder = Path(os.environ.get("OUTPUT_FOLDER", "./output"))
        folder.mkdir(parents=True, exist_ok=True)
        return folder

# --- Helpers ---
def normalize_indexer_name(name: str) -> str:
    first_part = name.split(" ")[0] if " " in name else name
    return re.sub(r"[^\w\-_.]", "", first_part).lower() or "unknown"

def get_headers(api_key: str) -> dict[str, str]:
    return {"X-Api-Key": api_key, "Content-Type": "application/json"}

def fetch_all_indexers(instance: dict[str, str]) -> Optional[list[dict]]:
    try:
        resp = requests.get(f"{instance['url']}/api/v3/indexer", headers=get_headers(instance["api_key"]), timeout=15)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"[{instance['name']}] Fetch failed: {e}")
        return None

def fetch_available_categories(instance: dict[str, str], indexer_config: dict) -> list[dict]:
    try:
        resp = requests.post(f"{instance['url']}/api/v3/indexer/action/newznabCategories", json=indexer_config, headers=get_headers(instance["api_key"]), timeout=15)
        resp.raise_for_status()
        return resp.json()["options"]
    except Exception as e:
        logger.error(f"[{instance['name']}] Categories fetch failed: {e}")
        return []

def update_indexer(instance: dict[str, str], idx_id: int, config: dict) -> bool:
    try:
        resp = requests.put(f"{instance['url']}/api/v3/indexer/{idx_id}", json=config, headers=get_headers(instance["api_key"]), timeout=15)
        resp.raise_for_status()
        return True
    except Exception as e:
        logger.error(f"[{instance['name']}] Update failed: {e}")
        return False

def extract_categories(indexer: dict) -> dict[str, list[int]]:
    cats = {"categories": [], "animeCategories": []}
    for field in indexer.get("fields", []):
        if field.get("name") in cats:
            cats[field["name"]] = field.get("value") or []
    return cats

# --- File Ops ---
def get_json_path(output: Path, inst_name: str, idx_name: str) -> Path:
    return (output / inst_name).mkdir(parents=True, exist_ok=True) or (output / inst_name / f"{normalize_indexer_name(idx_name)}.json")

def save_config(path: Path, raw: str, current: dict, available: list[dict]) -> None:
    data = {"raw_name": raw, "desired_categories": current, "available_categories": available}
    path.write_text(json.dumps(data, indent=2))
    logger.info(f"Created config: {path.name}")

def load_config(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text())
    except Exception as e:
        logger.error(f"Load failed {path}: {e}")
        return None

# --- Sync Logic ---
def sync_instance(instance: dict, output: Path, lock: threading.Lock) -> None:
    if not lock.acquire(blocking=False):
        return

    try:
        indexers = fetch_all_indexers(instance)
        if not indexers:
            return

        for idx in indexers:
            raw_name = idx.get("name", "")
            idx_id = idx.get("id")
            path = get_json_path(output, instance["name"], raw_name)
            current_cats = extract_categories(idx)

            if not path.exists():
                avail = fetch_available_categories(instance, idx)
                save_config(path, raw_name, current_cats, avail)
                continue

            cfg = load_config(path)
            if not cfg:
                continue

            desired = cfg.get("desired_categories", {})
            changed = False

            for key in ["categories", "animeCategories"]:
                curr_set = set(current_cats[key])
                des_set = set(desired.get(key, []))
                if curr_set != des_set:
                    logger.info(f"[{instance['name']}] Mismatch {raw_name} ({key}): {curr_set} -> {des_set}")
                    current_cats[key] = desired.get(key, [])
                    changed = True

            if changed:
                for field in idx.get("fields", []):
                    if field.get("name") in current_cats:
                        field["value"] = current_cats[field["name"]]

                if update_indexer(instance, idx_id, idx):
                    logger.info(f"[{instance['name']}] Synced {raw_name}")
                else:
                    logger.error(f"[{instance['name']}] Failed to sync {raw_name}")
    finally:
        lock.release()

# --- Event Handlers ---
class SignalRHandler:
    def __init__(self, instance: dict, output: Path, lock: threading.Lock):
        self.instance = instance
        self.output = output
        self.lock = lock
        self.conn = None

    def start(self):
        ws_url = f"{self.instance['url']}/signalr/messages?access_token={self.instance['api_key']}"
        self.conn = HubConnectionBuilder() \
            .with_url(ws_url, options={"verify_ssl": False, "headers": {"User-Agent": "badcat"}}) \
            .configure_logging(logging.WARNING) \
            .with_automatic_reconnect({"type": "raw", "keep_alive_interval": 10, "reconnect_interval": 180, "max_attempts": 10}) \
            .build()

        self.conn.on("receiveMessage", lambda msg: self._trigger(msg))

        while self.conn.transport.state.value not in [0, 1, 2]:
            try:
                self.conn.start()
                logger.info(f"[{self.instance['name']}] SignalR connected")
            except Exception as e:
                logger.error(f"[{self.instance['name']}] SignalR fail: {e}")
                time.sleep(3)

    def stop(self):
        if self.conn:
            self.conn.stop()

    def _trigger(self, msg):
        if isinstance(msg, list) and len(msg):
            msg = msg[0]
        else:
            return

        if "name" in msg and msg["name"] == "indexer" and not bouncy(self.instance["name"]):
            logger.info(f"[{self.instance['name']}] Indexer changed: {msg.get('body', {}).get('resource', {}).get('name')}")
            threading.Thread(target=sync_instance, args=(self.instance, self.output, self.lock), daemon=True).start()

class FileHandler(FileSystemEventHandler):
    def __init__(self, instances: dict, output: Path, locks: dict):
        self.instances = instances
        self.output = output
        self.locks = locks

    def on_modified(self, event):
        if event.is_directory or not event.src_path.endswith(".json"):
            return

        path = Path(event.src_path)
        try:
            rel = path.relative_to(self.output)
            inst_name = rel.parts[0]
        except ValueError:
            return

        if inst_name not in self.instances:
            return

        if bouncy(inst_name):
            return

        logger.info(f"Edit detected: {path.name}, syncing {inst_name}")
        threading.Thread(target=sync_instance, args=(self.instances[inst_name], self.output, self.locks[inst_name]), daemon=True).start()

# --- Main ---
class App:
    def __init__(self):
        self.shutdown = threading.Event()
        self.instances: dict = {}
        self.output: Path = Path()
        self.locks: dict[str, threading.Lock] = {}
        self.handlers: list[SignalRHandler] = []
        self.observer = None

        self.instances = ConfigManager.load_instances()
        self.output = ConfigManager.get_output_folder()
        self.locks = {i: threading.Lock() for i in self.instances.keys()}
        logger.info(f"Loaded {len(self.instances)} arr(s)")

    def run(self):
        signal.signal(signal.SIGINT, lambda s, f: self.shutdown.set())
        signal.signal(signal.SIGTERM, lambda s, f: self.shutdown.set())

        # Initial Sync
        for i, inst in self.instances.items():
            sync_instance(inst, self.output, self.locks[i])

        # Watchdog
        self.observer = Observer()
        self.observer.schedule(FileHandler(self.instances, self.output, self.locks), str(self.output), recursive=True)
        self.observer.start()

        # SignalR
        for i, inst in self.instances.items():
            h = SignalRHandler(inst, self.output, self.locks[i])
            h.start()
            self.handlers.append(h)

        logger.info("Running...")
        self.shutdown.wait()

        # Cleanup
        self.observer.stop()
        self.observer.join(timeout=5)
        for h in self.handlers: h.stop()
        logger.info("Shutdown complete.")

if __name__ == "__main__":
    try:
        App().run()
    except Exception as e:
        logger.critical(f"Fatal: {e}")
        sys.exit(1)
