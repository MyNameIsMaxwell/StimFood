# -*- coding: utf-8 -*-
import json
from typing import Any, Dict, Optional

import aiosqlite

from app.config import DB_PATH


class FSMStorage:
    """Простейшее хранилище состояний в SQLite: state (TEXT) + data (JSON)."""

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path

    async def init(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS fsm_states (
                    user_id INTEGER PRIMARY KEY,
                    state TEXT,
                    data TEXT
                )
            """)
            await db.commit()

    async def get_state(self, user_id: int) -> Optional[str]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT state FROM fsm_states WHERE user_id = ?", (user_id,)) as cur:
                row = await cur.fetchone()
                return row[0] if row else None

    async def set_state(self, user_id: int, state: Optional[str]):
        async with aiosqlite.connect(self.db_path) as db:
            if state is None:
                await db.execute("DELETE FROM fsm_states WHERE user_id = ?", (user_id,))
            else:
                await db.execute("""
                    INSERT INTO fsm_states (user_id, state, data)
                    VALUES (?, ?, COALESCE((SELECT data FROM fsm_states WHERE user_id = ?), '{}'))
                    ON CONFLICT(user_id) DO UPDATE SET state=excluded.state
                """, (user_id, state, user_id))
            await db.commit()

    async def get_data(self, user_id: int) -> Dict[str, Any]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT data FROM fsm_states WHERE user_id = ?", (user_id,)) as cur:
                row = await cur.fetchone()
                if row and row[0]:
                    try:
                        return json.loads(row[0])
                    except Exception:
                        return {}
                return {}

    async def update_data(self, user_id: int, **kwargs):
        data = await self.get_data(user_id)
        data.update(kwargs)
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO fsm_states (user_id, state, data)
                VALUES (?, NULL, ?)
                ON CONFLICT(user_id) DO UPDATE SET data=excluded.data
            """, (user_id, json.dumps(data, ensure_ascii=False)))
            await db.commit()

    async def clear(self, user_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM fsm_states WHERE user_id = ?", (user_id,))
            await db.commit()

