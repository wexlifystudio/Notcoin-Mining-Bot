"""Offline tests: fake chain, in-memory ledger, fake bot callback. Run: python -m unittest discover -s tests -v"""
import os
import sys
import unittest

os.environ["NOTX_TESTING"] = "1"
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app import create_app  # noqa: E402
from chain import Ambiguous, ChainError, NotBroadcast  # noqa: E402
from core import Config, Service  # noqa: E402
from store import MemoryStore  # noqa: E402

KEY = "k" * 32
CB = "c" * 32


class FakeChain:
    address = "UQFAKEHOTWALLET"

    def __init__(self):
        self.sent = []
        self.mode = "ok"      # ok | preflight_fail | not_broadcast | ambiguous | no_confirm | boom_after
        self.seqno = 5

    def parse_address(self, t):
        if not t.startswith("UQ") or len(t) != 48:
            raise ValueError("invalid TON address")
        return t

    def preflight(self, nano):
        if self.mode == "preflight_fail":
            raise ChainError("payout wallet has not enough NOT")

    def send_jetton(self, dest, nano, qid):
        if self.mode == "not_broadcast":
            raise NotBroadcast("node rejected")
        if self.mode == "ambiguous":
            raise Ambiguous("timeout")
        self.sent.append((dest, nano, qid))
        return self.seqno

    def wait_seqno(self, s, timeout=100):
        return self.mode != "no_confirm"

    def find_tx(self, qid):
        return "abcdef"

    def balances(self):
        return {"ton_nano": 5 * 10 ** 9, "not_nano": 10 ** 12, "seqno": 5}


def make(chain=None, store=None, webhook="https://hook.example/x", daily=1000, per=500):
    chain = chain or FakeChain()
    store = store or MemoryStore()
    calls = []

    def poster(url, payload):
        calls.append((url, payload))
        return 200

    cfg = Config(KEY, CB, webhook, max_per_payout_not=per, daily_max_not=daily)
    svc = Service(cfg, store, chain, poster=poster, start_worker=False)
    return svc, chain, store, calls, create_app(svc).test_client()


W = "UQ" + "a" * 46


def post(c, **kw):
    body = {"request_id": 1, "user_id": 77, "wallet": W, "amount": "18.5"}
    body.update(kw)
    return c.post("/payout", json=body, headers={"X-Api-Key": KEY})


class T(unittest.TestCase):
    def test_auth(self):
        svc, ch, st, calls, c = make()
        self.assertEqual(c.post("/payout", json={}).status_code, 401)
        self.assertEqual(c.post("/payout", json={}, headers={"X-Api-Key": "x" * 32}).status_code, 401)
        self.assertEqual(c.get("/status").status_code, 401)
        self.assertEqual(c.get("/health").status_code, 200)
        self.assertEqual(c.get("/status", headers={"X-Api-Key": KEY}).status_code, 200)

    def test_key_in_body_and_form_fallbacks(self):
        svc, ch, st, calls, c = make()
        r = c.post("/payout", data='{"request_id": 3, "user_id": 1, "wallet": "%s", "amount": "1", "key": "%s"}' % (W, KEY))
        self.assertEqual(r.status_code, 202)
        r = c.post("/payout", data={"request_id": 4, "user_id": 1, "wallet": W, "amount": "1", "key": KEY})
        self.assertEqual(r.status_code, 202)

    def test_happy_path_and_callback(self):
        svc, ch, st, calls, c = make()
        r = post(c)
        self.assertEqual(r.status_code, 202)
        svc.process(1)
        self.assertEqual(ch.sent, [(W, 18_500_000_000, 1)])
        d = st.get(1)
        self.assertEqual(d["status"], "paid")
        self.assertEqual(calls[-1][1], {"rid": "1", "st": "paid", "tx": "abcdef", "err": "", "ky": CB})
        self.assertTrue(st.get(1)["notified"])

    def test_idempotent_no_double_pay(self):
        svc, ch, st, calls, c = make()
        post(c)
        svc.process(1)
        r = post(c)
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.get_json()["duplicate"])
        svc.process(1)  # not queued any more
        self.assertEqual(len(ch.sent), 1)
        self.assertGreaterEqual(len(calls), 2)  # callback re-sent on duplicate

    def test_same_id_different_data(self):
        svc, ch, st, calls, c = make()
        post(c)
        self.assertEqual(post(c, amount="99").status_code, 409)

    def test_limits(self):
        svc, ch, st, calls, c = make(per=100, daily=150)
        self.assertEqual(post(c, amount="101").status_code, 422)
        self.assertEqual(post(c, request_id=1, amount="100").status_code, 202)
        r = post(c, request_id=2, amount="60")
        self.assertEqual(r.status_code, 422)
        self.assertIn("daily", r.get_json()["error"])
        self.assertEqual(post(c, request_id=3, amount="50").status_code, 202)

    def test_bad_input(self):
        svc, ch, st, calls, c = make()
        self.assertEqual(post(c, wallet="nope").status_code, 422)
        self.assertEqual(post(c, amount="-1").status_code, 400)
        self.assertEqual(post(c, amount="abc").status_code, 400)
        self.assertEqual(post(c, request_id="x").status_code, 400)
        self.assertEqual(post(c, amount="0.000000001").status_code, 422)

    def test_failed_before_broadcast(self):
        svc, ch, st, calls, c = make()
        ch.mode = "preflight_fail"
        post(c)
        svc.process(1)
        self.assertEqual(st.get(1)["status"], "failed")
        self.assertEqual(ch.sent, [])
        self.assertEqual(calls[-1][1]["st"], "failed")

    def test_not_broadcast_is_failed(self):
        svc, ch, st, calls, c = make()
        ch.mode = "not_broadcast"
        post(c)
        svc.process(1)
        self.assertEqual(st.get(1)["status"], "failed")

    def test_ambiguous_goes_to_review_never_retried(self):
        svc, ch, st, calls, c = make()
        ch.mode = "ambiguous"
        post(c)
        svc.process(1)
        self.assertEqual(st.get(1)["status"], "review")
        ch.mode = "ok"
        svc.process(1)
        self.assertEqual(ch.sent, [])

    def test_unconfirmed_goes_to_review(self):
        svc, ch, st, calls, c = make()
        ch.mode = "no_confirm"
        post(c)
        svc.process(1)
        self.assertEqual(st.get(1)["status"], "review")
        self.assertEqual(len(ch.sent), 1)

    def test_restart_with_sending_goes_to_review(self):
        store = MemoryStore()
        svc, ch, st, calls, c = make(store=store)
        post(c)
        store.cas_status(1, "queued", "sending")
        svc2, ch2, st2, calls2, c2 = make(store=store)
        self.assertEqual(store.get(1)["status"], "review")
        svc2.process(1)
        self.assertEqual(ch2.sent, [])

    def test_restart_requeues_queued(self):
        store = MemoryStore()
        svc, ch, st, calls, c = make(store=store)
        post(c)
        svc2, ch2, st2, calls2, c2 = make(store=store)
        self.assertEqual(svc2.q.qsize(), 1)

    def test_callback_error_text_is_sanitized(self):
        svc, ch, st, calls, c = make()
        ch.mode = "not_broadcast"
        post(c)
        st.update(1, {})
        svc.process(1)
        err = calls[-1][1]["err"]
        for bad in ['"', "'", ",", "{", "}"]:
            self.assertNotIn(bad, err)

    def test_status_endpoint(self):
        svc, ch, st, calls, c = make()
        post(c)
        r = c.get("/payout/1", headers={"X-Api-Key": KEY})
        self.assertEqual(r.get_json()["status"], "queued")
        self.assertEqual(c.get("/payout/99", headers={"X-Api-Key": KEY}).status_code, 404)

    def test_no_webhook_configured(self):
        svc, ch, st, calls, c = make(webhook="")
        post(c)
        svc.process(1)
        self.assertEqual(st.get(1)["status"], "paid")
        self.assertEqual(calls, [])
        self.assertFalse(st.get(1)["notified"])

    def test_short_api_key_disables_everything(self):
        cfg = Config("short", CB, "x")
        svc = Service(cfg, MemoryStore(), FakeChain(), start_worker=False)
        c = create_app(svc).test_client()
        r = c.post("/payout", json={}, headers={"X-Api-Key": "short"})
        self.assertEqual(r.status_code, 401)


if __name__ == "__main__":
    unittest.main()
