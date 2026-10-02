"""Payout service logic: validation, limits, idempotency, one-at-a-time sending, callbacks.

Safety rules (read before changing anything):
  * a request_id is paid at most once. State is stored before and after the broadcast.
  * a 'sending' record found after a restart is NEVER retried; it goes to 'review'.
  * 'failed' means nothing was broadcast. 'review' means a human must look at the wallet.
"""
import hmac
import logging
import queue
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from chain import ChainError, Ambiguous

log = logging.getLogger("payout")

TERMINAL = ("paid", "failed", "review")
COUNTED = ("queued", "sending", "paid", "review")  # amounts that count toward the daily cap
NANO = 10 ** 9


def utc_day():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def clean_text(s, n=120):
    s = str(s)
    for ch in ['"', "'", ",", "{", "}", "[", "]", "\n", "\r", "\\"]:
        s = s.replace(ch, " ")
    return " ".join(s.split())[:n]


class Config:
    def __init__(self, api_key, callback_key="", webhook_url="", max_per_payout_not=5000,
                 daily_max_not=50000, min_payout_not="0.01"):
        self.api_key = api_key
        self.callback_key = callback_key
        self.webhook_url = webhook_url
        self.max_per_payout_nano = int(Decimal(str(max_per_payout_not)) * NANO)
        self.daily_max_nano = int(Decimal(str(daily_max_not)) * NANO)
        self.min_payout_nano = int(Decimal(str(min_payout_not)) * NANO)


class Service:
    def __init__(self, cfg, store, chain, poster=None, start_worker=True):
        self.cfg = cfg
        self.store = store
        self.chain = chain
        self.poster = poster  # callable(url, json) -> status_code, used for bot callbacks
        self.q = queue.Queue()
        self.stop = False
        self._recover()
        if start_worker:
            threading.Thread(target=self._worker, daemon=True).start()
            threading.Thread(target=self._renotify_loop, daemon=True).start()

    # ------------------------------------------------------------------ auth
    def key_ok(self, given):
        if not self.cfg.api_key or len(self.cfg.api_key) < 24:
            return False
        return hmac.compare_digest(str(given or ""), self.cfg.api_key)

    # ------------------------------------------------------------------ recovery
    def _recover(self):
        # Anything stuck in 'sending' may or may not have been broadcast: a human must check.
        for d in self.store.find_status(("sending",)):
            self.store.update(d["_id"], {"status": "review", "error": "service restarted while sending - check the wallet",
                                         "updated_at": time.time(), "notified": False})
        for d in self.store.find_status(("queued",)):
            self.q.put(d["_id"])

    # ------------------------------------------------------------------ submit
    def submit(self, data):
        """Returns (http_status, body)."""
        try:
            rid = int(str(data.get("request_id")).strip())
            uid = int(str(data.get("user_id")).strip())
            wallet = str(data.get("wallet", "")).strip()
            amount = Decimal(str(data.get("amount")).strip())
            if rid <= 0 or uid <= 0 or amount <= 0 or amount != amount:
                raise ValueError()
        except (ValueError, InvalidOperation, TypeError):
            return 400, {"ok": False, "error": "bad request: need request_id, user_id, wallet, amount"}
        nano = int((amount * NANO).to_integral_value())
        if nano < self.cfg.min_payout_nano:
            return 422, {"ok": False, "status": "rejected", "error": "amount below the minimum payout"}
        if nano > self.cfg.max_per_payout_nano:
            return 422, {"ok": False, "status": "rejected", "error": "amount above the per payout limit"}

        existing = self.store.get(rid)
        if existing:
            if existing["user_id"] != uid or existing["wallet_in"] != wallet or existing["amount_nano"] != nano:
                return 409, {"ok": False, "error": "request_id already used with different data"}
            if existing["status"] == "failed":
                # 'failed' means nothing was broadcast, so trying again can never pay twice.
                # (An admin fixes the cause, e.g. deploys the wallet, then approves again.)
                day = utc_day()
                if self.store.sum_day(day, COUNTED) + nano > self.cfg.daily_max_nano:
                    return 422, {"ok": False, "status": "rejected", "error": "daily payout limit reached"}
                if self.store.cas_status(rid, "failed", "queued",
                                         {"error": "", "tx": "", "updated_at": time.time(), "day": day,
                                          "notified": False, "notify_tries": 0}):
                    self.q.put(rid)
                    return 202, {"ok": True, "status": "queued", "retry": True}
                return 200, {"ok": True, "status": existing["status"], "duplicate": True}
            if existing["status"] in TERMINAL:
                self.store.update(rid, {"notified": False})
                self._notify_async(rid)
            return 200, {"ok": True, "status": existing["status"], "duplicate": True}

        try:
            dest = self.chain.parse_address(wallet)
        except ValueError as ex:
            return 422, {"ok": False, "status": "rejected", "error": str(ex)}

        day = utc_day()
        used = self.store.sum_day(day, COUNTED)
        if used + nano > self.cfg.daily_max_nano:
            return 422, {"ok": False, "status": "rejected", "error": "daily payout limit reached"}

        now = time.time()
        doc = {"_id": rid, "user_id": uid, "wallet_in": wallet, "dest": dest, "amount_nano": nano,
               "status": "queued", "created_at": now, "updated_at": now, "day": day, "error": "",
               "tx": "", "seqno": None, "notified": False, "notify_tries": 0}
        if not self.store.insert_if_absent(doc):
            return 200, {"ok": True, "status": "queued", "duplicate": True}
        self.q.put(rid)
        return 202, {"ok": True, "status": "queued"}

    def public(self, rid):
        d = self.store.get(rid)
        if not d:
            return None
        return {"request_id": d["_id"], "status": d["status"], "error": d.get("error", ""), "tx": d.get("tx", ""),
                "amount": "%.9f" % (d["amount_nano"] / NANO), "notified": bool(d.get("notified"))}

    # ------------------------------------------------------------------ worker
    def _worker(self):
        while not self.stop:
            try:
                rid = self.q.get(timeout=2)
            except queue.Empty:
                continue
            try:
                self.process(rid)
            except Exception:
                log.exception("worker crashed on %s", rid)

    def _finish(self, rid, status, error="", tx=""):
        self.store.update(rid, {"status": status, "error": error, "tx": tx, "updated_at": time.time(), "notified": False})
        self._notify(rid)

    def process(self, rid):
        if not self.store.cas_status(rid, "queued", "sending", {"updated_at": time.time()}):
            return  # someone else has it, or it was already handled
        doc = self.store.get(rid)
        amount_nano = doc["amount_nano"]
        broadcast = False
        try:
            # re-check the daily cap with this request already counted once
            used = self.store.sum_day(doc["day"], COUNTED)
            if used > self.cfg.daily_max_nano:
                self._finish(rid, "failed", "daily payout limit reached")
                return
            self.chain.preflight(amount_nano)
            seqno = self.chain.send_jetton(doc["dest"], amount_nano, rid)
            broadcast = True
            self.store.update(rid, {"seqno": seqno, "broadcast_at": time.time()})
            if self.chain.wait_seqno(seqno):
                tx = ""
                try:
                    tx = self.chain.find_tx(rid)
                except Exception:
                    tx = ""
                self._finish(rid, "paid", "", tx)
            else:
                self._finish(rid, "review", "transfer sent but not confirmed in time - check the wallet")
        except Ambiguous as ex:
            self._finish(rid, "review", clean_text("broadcast unclear: %s" % ex))
        except ChainError as ex:
            if broadcast:
                self._finish(rid, "review", clean_text(ex))
            else:
                self._finish(rid, "failed", clean_text(ex))
        except Exception as ex:
            log.exception("payout %s", rid)
            if broadcast:
                self._finish(rid, "review", clean_text("unexpected: %s" % ex))
            else:
                self._finish(rid, "review", clean_text("unexpected before confirm: %s" % ex))

    # ------------------------------------------------------------------ callbacks to the bot
    def _notify_async(self, rid):
        threading.Thread(target=self._notify, args=(rid,), daemon=True).start()

    def _notify(self, rid):
        d = self.store.get(rid)
        if not d or d["status"] not in TERMINAL:
            return
        if not self.cfg.webhook_url or not self.poster:
            return
        payload = {"rid": str(rid), "st": d["status"], "tx": d.get("tx", ""), "err": clean_text(d.get("error", "")),
                   "ky": self.cfg.callback_key}
        tries = int(d.get("notify_tries", 0)) + 1
        ok = False
        try:
            code = self.poster(self.cfg.webhook_url, payload)
            ok = 200 <= int(code) < 300
        except Exception as ex:
            log.warning("callback for %s failed: %s", rid, ex)
        self.store.update(rid, {"notified": ok, "notify_tries": tries})

    def _renotify_loop(self):
        while not self.stop:
            time.sleep(60)
            try:
                for d in self.store.find_unnotified(TERMINAL, 30):
                    self._notify(d["_id"])
            except Exception:
                log.exception("renotify")

    # ------------------------------------------------------------------ admin view
    def overview(self):
        out = {"wallet": self.chain.address, "today_used_not": "%.4f" % (self.store.sum_day(utc_day(), COUNTED) / NANO),
               "daily_max_not": "%.4f" % (self.cfg.daily_max_nano / NANO),
               "per_payout_max_not": "%.4f" % (self.cfg.max_per_payout_nano / NANO),
               "review": len(self.store.find_status(("review",))),
               "queued": len(self.store.find_status(("queued", "sending")))}
        try:
            b = self.chain.balances()
            out["ton"] = "%.4f" % (b["ton_nano"] / NANO)
            out["not"] = "%.4f" % (b["not_nano"] / NANO)
            out["deployed"] = b["seqno"] is not None
        except Exception as ex:
            out["chain_error"] = clean_text(ex)
        return out
