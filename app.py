"""NOTX MINER payout backend (Flask). Start: gunicorn app:app --workers 1 --threads 8"""
import logging
import os
import time

import requests
from flask import Flask, jsonify, request

from core import Config, Service

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("app")


def _env(name, default=""):
    return os.environ.get(name, default).strip()


def build_service():
    api_key = _env("API_KEY")
    if len(api_key) < 24:
        log.error("API_KEY must be at least 24 characters. Payouts are disabled.")
    cfg = Config(
        api_key=api_key,
        callback_key=_env("CALLBACK_KEY"),
        webhook_url=_env("BOT_WEBHOOK_URL"),
        max_per_payout_not=_env("MAX_PER_PAYOUT_NOT", "5000"),
        daily_max_not=_env("DAILY_MAX_NOT", "50000"),
        min_payout_not=_env("MIN_PAYOUT_NOT", "0.01"),
    )
    uri = _env("MONGODB_URI")
    if not uri:
        raise SystemExit("MONGODB_URI is required (the ledger must survive restarts).")
    from store import MongoStore
    store = MongoStore(uri, _env("MONGODB_DB", "notx_payout"))

    from chain import TonChain, NOT_MASTER
    chain = TonChain(
        mnemonic=_env("WALLET_MNEMONIC"),
        api_key=_env("TONCENTER_API_KEY"),
        base_url=_env("TONCENTER_URL", "https://toncenter.com"),
        jetton_master=_env("NOT_MASTER", NOT_MASTER),
        gas_nano=int(float(_env("GAS_TON", "0.07")) * 10 ** 9),
        version=_env("WALLET_VERSION", "v4r2"),
    )

    def poster(url, payload):
        return requests.post(url, json=payload, timeout=(6, 15)).status_code

    return Service(cfg, store, chain, poster=poster, start_worker=True)


def create_app(service):
    app = Flask(__name__)

    @app.before_request
    def _keep_background_threads_alive():
        service.ensure_started()

    def body():
        data = request.get_json(force=True, silent=True)
        if not isinstance(data, dict):
            data = request.form.to_dict() if request.form else {}
        return data

    def authed(data=None):
        key = request.headers.get("X-Api-Key") or (data or {}).get("key") or request.args.get("key")
        return service.key_ok(key)

    @app.get("/")
    @app.get("/health")
    def health():
        return jsonify({"ok": True, "service": "notx-payout", "wallet": service.chain.address})

    @app.get("/status")
    def status():
        if not authed():
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        return jsonify({"ok": True, **service.overview()})

    @app.get("/payouts")
    def payouts():
        if not authed():
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        return jsonify({"ok": True, "payouts": service.recent(20)})

    @app.get("/cancel/<int:rid>")
    def cancel(rid):
        if not authed():
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        code, out = service.cancel(rid)
        return jsonify(out), code

    @app.get("/debug")
    def debug():
        """Where is every thread right now? Opens a stuck-worker mystery in one page."""
        if not authed():
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        import sys
        import threading
        import traceback
        names = {t.ident: t.name for t in threading.enumerate()}
        stacks = {}
        for ident, frame in sys._current_frames().items():
            lines = traceback.format_stack(frame)[-4:]
            stacks["%s (%s)" % (names.get(ident, "?"), ident)] = [x.strip().replace("\n", " | ") for x in lines]
        cur = service.current
        import os
        return jsonify({"ok": True, "worker_alive": bool(service.worker and service.worker.is_alive()),
                        "threads_alive": service.threads_alive(), "pid": os.getpid(), "service_pid": service.pid,
                        "restarts": service.restarts, "queue_len": service.q.qsize(),
                        "busy_with": cur, "seconds_in_step": int(time.time() - cur["since"]) if cur else None,
                        "threads": stacks})

    @app.post("/payout")
    def payout():
        data = body()
        if not authed(data):
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        code, out = service.submit(data)
        return jsonify(out), code

    @app.get("/payout/<int:rid>")
    def payout_status(rid):
        if not authed():
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        out = service.public(rid)
        if not out:
            return jsonify({"ok": False, "error": "not found"}), 404
        return jsonify({"ok": True, **out})

    return app


if os.environ.get("NOTX_TESTING") != "1":
    app = create_app(build_service())
