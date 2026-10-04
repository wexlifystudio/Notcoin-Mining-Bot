"""TON side: sends the NOT jetton from the hot wallet.

Uses tonsdk (build + sign) and the public Toncenter HTTP API (balances, seqno,
broadcast). Nothing here is imported unless the real chain is used, so the rest
of the service can be tested without tonsdk installed.
"""
import base64
import logging
import threading
import time

import requests

log = logging.getLogger("chain")

NOT_MASTER = "EQAvlWFDxGF2lXm67y4yzC17wYKD9A0guwPkMs1gOsM__NOT"  # Notcoin jetton master, 9 decimals


class ChainError(Exception):
    """Failed before anything was broadcast. Safe to treat as 'not paid'."""


class NotBroadcast(ChainError):
    pass


class Ambiguous(Exception):
    """The broadcast may or may not have reached the network. Never retry automatically."""


class TonChain:
    def __init__(self, mnemonic, api_key="", base_url="https://toncenter.com",
                 jetton_master=NOT_MASTER, gas_nano=70_000_000, version="v4r2"):
        from tonsdk.contract.wallet import Wallets, WalletVersionEnum
        ver = {"v3r2": WalletVersionEnum.v3r2, "v4r2": WalletVersionEnum.v4r2}.get(version)
        if ver is None:
            raise ValueError("WALLET_VERSION must be v3r2 or v4r2")
        words = mnemonic.split()
        if len(words) != 24:
            raise ValueError("WALLET_MNEMONIC must have 24 words")
        _m, _pub, _priv, self.wallet = Wallets.from_mnemonics(words, ver, 0)
        self.address = self.wallet.address.to_string(True, True, False)
        self.base = base_url.rstrip("/")
        self.master = jetton_master
        self.gas_nano = int(gas_nano)
        self.http = requests.Session()
        if api_key:
            self.http.headers["X-API-Key"] = api_key
        self._jw = None
        # Toncenter allows ~1 request/second without a key (about 10/s with one). Going faster only
        # produces HTTP 429 and long retry sleeps, so every call to it is spaced out here.
        self._gap = 0.15 if api_key else 1.15
        self._gate = threading.Lock()
        self._last_call = 0.0

    def _pace(self):
        with self._gate:
            wait = self._last_call + self._gap - time.time()
            if wait > 0:
                time.sleep(wait)
            self._last_call = time.time()

    # ---------------------------------------------------------------- http helpers
    def _get(self, path, params=None, tries=4):
        last = None
        for i in range(tries):
            try:
                self._pace()
                r = self.http.get(self.base + path, params=params, timeout=(6, 15))
                if r.status_code == 429:
                    log.warning("toncenter 429 on %s (try %d)", path, i + 1)
                    time.sleep(1.5 * (i + 1))
                    continue
                r.raise_for_status()
                return r.json()
            except Exception as ex:  # network or http error
                last = ex
                time.sleep(1.0 * (i + 1))
        raise ChainError("toncenter request failed: %s" % last)

    # ---------------------------------------------------------------- address / state
    def parse_address(self, text):
        from tonsdk.utils import Address
        try:
            a = Address(text.strip())
        except Exception:
            raise ValueError("invalid TON address")
        if getattr(a, "wc", 0) != 0:
            raise ValueError("only workchain 0 addresses are supported")
        if a.to_string(False) == self.wallet.address.to_string(False):
            raise ValueError("destination is the payout wallet itself")
        return a.to_string(True, True, False)

    def wallet_info(self):
        j = self._get("/api/v2/getWalletInformation", {"address": self.address})
        res = j.get("result") or {}
        seq = res.get("seqno")
        return {"balance": int(res.get("balance") or 0), "seqno": (int(seq) if seq is not None else None),
                "state": res.get("account_state", "")}

    def jetton_wallet(self):
        if self._jw:
            return self._jw
        j = self._get("/api/v3/jetton/wallets",
                      {"owner_address": self.address, "jetton_address": self.master, "limit": 1})
        arr = j.get("jetton_wallets") or []
        if not arr:
            raise ChainError("the payout wallet holds no NOT jetton wallet yet (send some NOT to it first)")
        self._jw = {"address": arr[0]["address"], "balance": int(arr[0].get("balance") or 0)}
        return self._jw

    def balances(self):
        info = self.wallet_info()
        self._jw = None
        jw = self.jetton_wallet()
        return {"ton_nano": info["balance"], "not_nano": jw["balance"], "seqno": info["seqno"]}

    def preflight(self, amount_nano):
        b = self.balances()
        if b["seqno"] is None:
            raise ChainError("payout wallet is not deployed yet (make one small TON transfer from it first)")
        if b["not_nano"] < amount_nano:
            raise ChainError("payout wallet has not enough NOT")
        if b["ton_nano"] < self.gas_nano + 20_000_000:
            raise ChainError("payout wallet has not enough TON for gas")
        return b

    # ---------------------------------------------------------------- send
    def send_jetton(self, dest_friendly, amount_nano, query_id):
        """Build, sign and broadcast one jetton transfer. Returns the seqno that was used."""
        try:
            from tonsdk.contract.token.ft import JettonWallet
            from tonsdk.utils import Address, bytes_to_b64str
            info = self.wallet_info()
            if info["seqno"] is None:
                raise NotBroadcast("payout wallet is not deployed")
            seqno = info["seqno"]
            jw = self.jetton_wallet()
            body = JettonWallet().create_transfer_body(
                Address(dest_friendly), int(amount_nano),
                forward_amount=0, response_address=self.wallet.address, query_id=int(query_id))
            q = self.wallet.create_transfer_message(jw["address"], self.gas_nano, seqno, payload=body)
            boc = bytes_to_b64str(q["message"].to_boc(False))
        except ChainError:
            raise
        except Exception as ex:
            raise NotBroadcast("could not build the transfer: %s" % ex)

        try:
            self._pace()
            r = self.http.post(self.base + "/api/v2/sendBoc", json={"boc": boc}, timeout=(6, 25))
        except Exception as ex:
            raise Ambiguous("broadcast request failed: %s" % ex)
        if r.status_code >= 500 or r.status_code == 429:
            raise Ambiguous("toncenter answered HTTP %d" % r.status_code)
        try:
            j = r.json()
        except Exception:
            raise Ambiguous("toncenter answered with unreadable data (HTTP %d)" % r.status_code)
        if r.status_code >= 400 or not j.get("ok"):
            raise NotBroadcast("node rejected the transfer: %s" % str(j.get("error", j))[:120])
        return seqno

    def wait_seqno(self, used_seqno, timeout=100):
        end = time.time() + timeout
        while time.time() < end:
            time.sleep(4)
            try:
                s = self.wallet_info()["seqno"]
            except Exception:
                continue
            if s is not None and s > used_seqno:
                return True
        return False

    def find_tx(self, query_id, budget=20):
        """Best effort, never blocks the payout queue for long: look the transfer up by its query_id
        for at most `budget` seconds. Returns a hex tx hash or ''."""
        end = time.time() + budget
        while True:
            try:
                j = self._get("/api/v3/jetton/transfers",
                              {"owner_address": self.address, "direction": "out", "limit": 20, "sort": "desc"}, tries=1)
                for t in j.get("jetton_transfers") or []:
                    if str(t.get("query_id")) == str(query_id):
                        h = str(t.get("transaction_hash") or "")
                        if len(h) == 44:
                            try:
                                h = base64.b64decode(h).hex()
                            except Exception:
                                pass
                        return h
            except Exception:
                pass
            if time.time() + 4 >= end:
                return ""
            time.sleep(4)
