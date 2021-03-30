import node_message
import node_info
import json
import sqlite3
import os
import uuid


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
    print("Opened database {} successfully".format(db_name))
    cursor = conn.cursor()
    setup_db()


init_db("message.db")


def remove_db(db_name="test.db"):
    conn.close()
    os.remove(db_name)
    print("Closed and removed database {} successfully".format(db_name))


def store_messages(messages: []):
    msg_objects = node_message.message_bulk_to_message_array(messages)

    msg_entries = [(msg.__hash__(), msg.sender.hex, msg.seq_number, msg.message_content, False) for msg in msg_objects]

    # print(msg_entries)

    cursor.executemany("""
        INSERT OR IGNORE INTO message VALUES (?,?,?,?,?);
    """, msg_entries)


def store_messages_in_pool(messages: [], pool):
    msg_objects = node_message.message_bulk_to_message_array(messages)
    
    msg_entries = [(msg.__hash__(), pool.hex) for msg in msg_objects]

    # print(msg_entries)

    cursor.executemany("""
        INSERT OR IGNORE INTO message_pool VALUES (?,?);
    """, msg_entries)


def get_message_for_pool(msg_pool):
    pool_entry = (msg_pool.hex,)

    cursor.execute("""
        SELECT sender, seq_number, message FROM message JOIN message_pool ON message.msg_hash = message_pool.msg_hash WHERE message_pool.msg_pool = ?;
    """, pool_entry)

    msg_entries = cursor.fetchall()
    messages = [node_message.make_queue_message(uuid.UUID(hex=msg_entry[0]), msg_entry[1], msg_entry[2]) for msg_entry in msg_entries]
    return messages


# def store_round(seq):
#     seq = (seq,)

#     cursor.execute("""
#         UPDATE message SET confirmed = 1 WHERE seq_number = ?;
#     """, seq)
