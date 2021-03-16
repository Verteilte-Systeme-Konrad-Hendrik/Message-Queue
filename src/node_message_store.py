import node_message
import json


# contains rounds that have not been completed yet
pending_rounds = {}


def store_messages(messages: []):
    msg_objects = node_message.message_bulk_to_message_array(messages)

    for msg_obj in msg_objects:
        store_message(msg_obj)


def store_message(msg):
    if msg.seq_number not in pending_rounds:
        pending_rounds[msg.seq_number] = {}

    if msg.sender not in pending_rounds[msg.seq_number]:
        pending_rounds[msg.seq_number][msg.sender] = msg
        # trigger new message received
    else:
        if hash(pending_rounds[msg.seq_number][msg.sender]) == hash(msg):
            # same message delivered twice so no need to write
            pass
        else:
            raise Exception(str(msg.sender) + " tried to deliver two different messages in one sequence!")


def store_round(round, seq):
    with open('seq_' + str(seq) + '.txt', 'w') as file:
        file.write(json.dumps(round))
