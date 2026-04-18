# -*- coding: utf-8 -*-
import json
from typing import List, Optional, Tuple

import aiosqlite

from app.config import DB_PATH
from app.utils import now_msk_str
from app.sheets import sheets_append_poll_answer, sheets_find_client


class PollStore:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path

    async def init_tables(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS poll_batches (
                    batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    question TEXT NOT NULL,
                    options TEXT NOT NULL, -- JSON array of strings
                    created_at TEXT NOT NULL,
                    created_by INTEGER NOT NULL
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS poll_ids (
                    poll_id TEXT PRIMARY KEY,
                    batch_id INTEGER NOT NULL,
                    chat_id INTEGER NOT NULL,
                    FOREIGN KEY(batch_id) REFERENCES poll_batches(batch_id)
                )
            """)
            await db.commit()

    async def create_batch(self, question: str, options: List[str], created_by: int) -> int:
        created_at = now_msk_str()
        options_json = json.dumps(options, ensure_ascii=False)
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                "INSERT INTO poll_batches (question, options, created_at, created_by) VALUES (?, ?, ?, ?)",
                (question, options_json, created_at, created_by)
            )
            await db.commit()
            return cur.lastrowid

    async def add_poll_id(self, poll_id: str, batch_id: int, chat_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT OR REPLACE INTO poll_ids (poll_id, batch_id, chat_id) VALUES (?, ?, ?)",
                (poll_id, batch_id, chat_id)
            )
            await db.commit()

    async def get_batch_by_poll_id(self, poll_id: str) -> Optional[Tuple[int, str, List[str], int]]:
        """
        Returns (batch_id, question, options_list, chat_id) for given poll_id or None.
        """
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("""
                SELECT b.batch_id, b.question, b.options, p.chat_id
                FROM poll_ids p
                JOIN poll_batches b ON b.batch_id = p.batch_id
                WHERE p.poll_id = ?
            """, (poll_id,)) as cur:
                row = await cur.fetchone()
                if not row:
                    return None
                batch_id, question, options_json, chat_id = row
                try:
                    options_list = json.loads(options_json) if options_json else []
                except Exception:
                    options_list = []
                return batch_id, question, options_list, chat_id


_poll_store: Optional[PollStore] = None


def get_poll_store() -> PollStore:
    global _poll_store
    if _poll_store is None:
        _poll_store = PollStore(DB_PATH)
    return _poll_store


async def record_poll_answers(user_id: int, question: str, answers: List[str]):
    """Append one sheet row per selected option (same date/name/username for each)."""
    answers = [a.strip() for a in answers if a and str(a).strip()]
    if not answers:
        return
    client = await sheets_find_client(user_id)
    name = str(client.get("Имя", "")).strip() if client else ""
    username = str(client.get("Username", "")).strip() if client else ""
    for answer in answers:
        await sheets_append_poll_answer(
            chat_id=user_id,
            name=name,
            username=username,
            question=question,
            answer=answer,
        )


async def record_poll_answer(user_id: int, question: str, answer: str):
    """
    Resolve name/username from Sheets if available and append answer to \"Опросы\" sheet.
    """
    await record_poll_answers(user_id, question, [answer])

