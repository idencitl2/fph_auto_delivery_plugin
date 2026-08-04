import asyncio
import os
import sqlite3
import time
from funpaybotengine.plugin import BasePlugin
from funpaybotengine.models import Order

# Try importing psycopg2 for Neon (PostgreSQL). If missing, fallback gracefully to SQLite.
try:
    import psycopg2
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False


class AutoWorkflowPlugin(BasePlugin):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Check for Neon PostgreSQL database URL in environment variables
        self.db_url = os.getenv("DATABASE_URL")
        # Fallback SQLite path if DATABASE_URL is not set
        self.db_path = os.path.join(os.path.dirname(__file__), "..", "orders.db")

    def get_db_connection(self):
        """Returns a PostgreSQL connection if DATABASE_URL is present, else SQLite."""
        if self.db_url and HAS_PSYCOPG2:
            return psycopg2.connect(self.db_url)
        return sqlite3.connect(self.db_path)

    async def on_activate(self):
        """Runs immediately when FunPay Hub starts the plugin."""
        # Step 1: Initialize the database tables
        await asyncio.to_thread(self.init_db)
        
        # Step 2: Start the background monitor loop
        self.create_task(self.monitor_6_hour_pings())

    # --- DATABASE OPERATIONS (Thread-Safe & Non-blocking) ---

    def init_db(self):
        """Creates the database and tables if they don't exist yet."""
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            if self.db_url and HAS_PSYCOPG2:
                # PostgreSQL Syntax
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS orders (
                        order_id VARCHAR(255) PRIMARY KEY,
                        chat_id VARCHAR(255) NOT NULL,
                        timestamp DOUBLE PRECISION NOT NULL,
                        reminded INTEGER DEFAULT 0
                    )
                """)
            else:
                # SQLite Syntax
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS orders (
                        order_id TEXT PRIMARY KEY,
                        chat_id TEXT NOT NULL,
                        timestamp REAL NOT NULL,
                        reminded INTEGER DEFAULT 0
                    )
                """)
            conn.commit()

    def db_save_order(self, order_id: str, chat_id: str, timestamp: float):
        """Inserts or updates an order in the tracking database."""
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            if self.db_url and HAS_PSYCOPG2:
                cursor.execute(
                    """
                    INSERT INTO orders (order_id, chat_id, timestamp, reminded)
                    VALUES (%s, %s, %s, 0)
                    ON CONFLICT (order_id) DO UPDATE 
                    SET chat_id = EXCLUDED.chat_id, timestamp = EXCLUDED.timestamp, reminded = 0
                    """,
                    (order_id, chat_id, timestamp)
                )
            else:
                cursor.execute(
                    "INSERT OR REPLACE INTO orders (order_id, chat_id, timestamp, reminded) VALUES (?, ?, ?, 0)",
                    (order_id, chat_id, timestamp)
                )
            conn.commit()

    def db_get_pending_orders(self) -> list:
        """Retrieves all orders that have not been reminded yet."""
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT order_id, chat_id, timestamp FROM orders WHERE reminded = 0")
            return cursor.fetchall()

    def db_mark_as_reminded(self, order_id: str):
        """Marks an order as reminded so we don't ping them again."""
        param = "%s" if (self.db_url and HAS_PSYCOPG2) else "?"
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"UPDATE orders SET reminded = 1 WHERE order_id = {param}", (order_id,))
            conn.commit()

    # --- CORE WORKFLOW LOGIC ---

    async def on_new_order(self, order: Order):
        """
        Automatically triggers when a new order is paid.
        """
        pool = self.settings.get("credentials_pool", [])
        if not pool:
            self.logger.error(f"CRITICAL: Could not deliver order {order.id}. 'credentials_pool' is empty or missing!")
            return

        # Pop credential and update settings persistently
        credentials = pool.pop(0)
        self.settings["credentials_pool"] = pool
        if hasattr(self, "save_settings"):
            await self.save_settings()

        delivery_msg = f"Thank you for your purchase!\nHere are your details:\n{credentials}"
        
        try:
            # Deliver item and restore listing
            await self.bot.chats.send_message(order.chat_id, delivery_msg)
            await self.bot.lots.restore_lot(order.lot_id)
            
            # Save tracking record to Neon / SQLite
            current_time = time.time()
            await asyncio.to_thread(self.db_save_order, order.id, order.chat_id, current_time)
            self.logger.info(f"Successfully processed and saved order {order.id}.")
        except Exception as e:
            self.logger.error(f"Failed to deliver order {order.id}: {e}")

    async def monitor_6_hour_pings(self):
        """
        Monitors database periodically and pings buyers when the timeout threshold is reached.
        """
        # Production values: wait_seconds = 21600 (6 hrs), loop_check_interval = 60
        wait_seconds = 10  # Set to 21600 for production (6 hours)
        loop_check_interval = 2  # Set to 60 for production
        
        while True:
            try:
                current_time = time.time()
                pending_orders = await asyncio.to_thread(self.db_get_pending_orders)
                
                for order_id, chat_id, timestamp in pending_orders:
                    elapsed_time = current_time - timestamp
                    
                    if elapsed_time >= wait_seconds:
                        try:
                            # Fetch live status from FunPay API
                            live_order = await self.bot.api.get_order_info(order_id)
                            
                            if live_order and getattr(live_order, "status", None) != "confirmed":
                                ping_msg = "Hi! Please check and confirm the order if all is well. 😊"
                                await self.bot.chats.send_message(chat_id, ping_msg)
                                self.logger.info(f"Sent reminder for Order {order_id}.")
                            
                            # Mark as reminded in DB
                            await asyncio.to_thread(self.db_mark_as_reminded, order_id)
                        except Exception as e:
                            self.logger.error(f"Error checking status for order {order_id}: {e}")

            except Exception as e:
                self.logger.error(f"Error in background ping monitor loop: {e}")

            await asyncio.sleep(loop_check_interval)
