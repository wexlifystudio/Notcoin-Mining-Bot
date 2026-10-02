"""Payout ledger storage.

MongoStore is the production store (survives Render restarts, atomic status
changes). MemoryStore exists only for tests and is refused in production.
"""
import threading


class MemoryStore:
    def __init__(self):
        self.docs = {}
        self.lock = threading.Lock()

    def insert_if_absent(self, doc):
        with self.lock:
            if doc["_id"] in self.docs:
                return False
            self.docs[doc["_id"]] = dict(doc)
            return True

    def get(self, rid):
        with self.lock:
            d = self.docs.get(rid)
            return dict(d) if d else None

    def update(self, rid, fields):
        with self.lock:
            if rid in self.docs:
                self.docs[rid].update(fields)

    def cas_status(self, rid, expected, new, fields=None):
        with self.lock:
            d = self.docs.get(rid)
            if not d or d["status"] != expected:
                return False
            d["status"] = new
            if fields:
                d.update(fields)
            return True

    def find_status(self, statuses):
        with self.lock:
            return [dict(d) for d in self.docs.values() if d["status"] in statuses]

    def find_unnotified(self, terminal, max_tries):
        with self.lock:
            return [
                dict(d) for d in self.docs.values()
                if d["status"] in terminal and not d.get("notified") and d.get("notify_tries", 0) < max_tries
            ]

    def sum_day(self, day, statuses):
        with self.lock:
            return sum(d["amount_nano"] for d in self.docs.values() if d.get("day") == day and d["status"] in statuses)


class MongoStore:
    def __init__(self, uri, db_name="notx_payout"):
        from pymongo import MongoClient
        from pymongo.errors import DuplicateKeyError
        self._dup = DuplicateKeyError
        self.col = MongoClient(uri, serverSelectionTimeoutMS=8000)[db_name]["payouts"]
        self.col.create_index("status")
        self.col.create_index("day")

    def insert_if_absent(self, doc):
        try:
            self.col.insert_one(doc)
            return True
        except self._dup:
            return False

    def get(self, rid):
        return self.col.find_one({"_id": rid})

    def update(self, rid, fields):
        self.col.update_one({"_id": rid}, {"$set": fields})

    def cas_status(self, rid, expected, new, fields=None):
        upd = dict(fields or {})
        upd["status"] = new
        r = self.col.update_one({"_id": rid, "status": expected}, {"$set": upd})
        return r.modified_count == 1

    def find_status(self, statuses):
        return list(self.col.find({"status": {"$in": list(statuses)}}))

    def find_unnotified(self, terminal, max_tries):
        return list(self.col.find({
            "status": {"$in": list(terminal)},
            "notified": {"$ne": True},
            "notify_tries": {"$not": {"$gte": max_tries}},
        }))

    def sum_day(self, day, statuses):
        total = 0
        for d in self.col.find({"day": day, "status": {"$in": list(statuses)}}, {"amount_nano": 1}):
            total += int(d["amount_nano"])
        return total
