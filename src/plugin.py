import asyncio
import os
import sqlite3
import time
from funpaybotengine.plugin import BasePlugin
from funpaybotengine.models import Order

class AutoWorkflowPlugin(BasePlugin):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Define the path to the database file inside the plugin folder
        self.db_path = os.path.join(os.path.dirname(__file__), "..", "orders.db")

    async def on_activate(self):
        """Runs immediately when FunPay Hub starts the plugin."""
        # Step 1: Initialize the database tables
        await asyncio.to_thread(self.init_db)
        
        # Step 2: Start the background 6-hour monitor loop
        self.create_task(self.monitor_6_hour_pings())

    # --- SQLITE DATABASE OPERATIONS (Thread-Safe & Non-blocking) ---

    def init_db(self):
        """Creates the database and tables if they don't exist yet."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
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
        """Inserts a new order into the tracking database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO orders (order_id, chat_id, timestamp, reminded) VALUES (?, ?, ?, 0)",
                (order_id, chat_id, timestamp)
            )
            conn.commit()

    def db_get_pending_orders(self) -> list:
        """Retrieves all orders that have not been reminded yet."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT order_id, chat_id, timestamp FROM orders WHERE reminded = 0")
            return cursor.fetchall()

    def db_mark_as_reminded(self, order_id: str):
        """Marks an order as reminded so we don't ping them again."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE orders SET reminded = 1 WHERE order_id = ?", (order_id,))
            conn.commit()

    # --- CORE WORKFLOW LOGIC ---

    async def on_new_order(self, order: Order):
        """
        Step 1, 2 & 4: Automatically triggers when a new order is paid.
        """
        # Safety Check: Safely pull the stock pool without crashing if empty
        pool = self.settings.get("credentials_pool", [])
        if not pool:
            self.logger.error(f"CRITICAL: Could not deliver order {order.id}. 'credentials_pool' is empty or missing!")
            return

        credentials = pool.pop(0)
        delivery_msg = f"Thank you for your purchase!\nHere are your details:\n{credentials}"
        await self.bot.chats.send_message(order.chat_id, delivery_msg)
        
        # Instantly re-list the item back on FunPay
        await self.bot.lots.restore_lot(order.lot_id)
        
        # Save the order to SQLite in a non-blocking thread
        current_time = time.time()
        await asyncio.to_thread(self.db_save_order, order.id, order.chat_id, current_time)
        self.logger.info(f"Saved order {order.id} to SQLite database.")

    async def monitor_6_hour_pings(self):
        """
        TEST VERSION: Checks every 2 seconds for a 10-second threshold.
        To switch to production: change test_wait_seconds to 21600 and loop_check_interval to 60.
        """
        test_wait_seconds = 10  
        loop_check_interval = 2  
        
        while True:
            current_time = time.time()
            
            # Retrieve un-reminded orders from the database
            pending_orders = await asyncio.to_thread(self.db_get_pending_orders)
            
            for order_id, chat_id, timestamp in pending_orders:
                # Calculate how much time has actually passed
                elapsed_time = current_time - timestamp
                
                if elapsed_time >= test_wait_seconds:
                    # Fetch live status from FunPay Hub
                    live_order = await self.bot.api.get_order_info(order_id)
                    
                    if live_order.status != "confirmed":
                        # Send the test nudge message
                        ping_msg = "Hi! Please check and confirm the order if all is well. 😊"
                        await self.bot.chats.send_message(chat_id, ping_msg)
                        self.logger.info(f"TEST: Sent 10-second reminder for Order {order_id}.")
                    
                    # Mark as reminded in the DB to clear it from tracking
                    await asyncio.to_thread(self.db_mark_as_reminded, order_id)
            
            # Check the database rapidly for quick local testing
            await asyncio.sleep(loop_check_interval)
