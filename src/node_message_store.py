import node_message
import node_info
import json
import sqlite3
import os

# contains rounds that have not been completed yet
pending_rounds = {}

conn = None
cursor = None

def setup_db():
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS message(msg_hash VARCHAR(64) PRIMARY KEY, sender VARCHAR(36), seq_number UNSIGNED BIG INT, message BLOB, confirmed BOOLEAN, UNIQUE(sender, seq_number));
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS message_pool(msg_hash VARCHAR(64) PRIMARY KEY, msg_pool VARCHAR(36));
    """)


def init_db(db_name="test.db"):
    global conn, cursor
    conn = sqlite3.connect(db_name)
    print("Opened database successfully")
    cursor = conn.cursor()
    setup_db()

init_db("message.db")

def remove_db(db_name="test.db"):
    conn.close()
    os.remove(db_name)

def store_messages(messages: []):
    msg_objects = node_message.message_bulk_to_message_array(messages)

    messages = [(msg.__hash__, msg.sender, msg.seq_number, msg.message_content, False) for msg in msg_objects]

    cursor.executemany("""
        INSERT INTO message VALUES (?,?,?,?,?)
    """, messages)


def get_message_for_pool(msg_pool):
    msg_pool = (msg_pool,)

    cursor.execute("""
        SELECT sender, seq_number, message FROM message JOIN message_pool ON message.msg_hash = message_pool.msg_hash WHERE message_pool.msg_pool = ?;
    """, msg_pool)

    return cursor.fetchall()


def store_round(seq):
    seq = (seq,)

    cursor.execute("""
        UPDATE message SET confirmed = 1 WHERE seq_number = ?;
    """, seq)
