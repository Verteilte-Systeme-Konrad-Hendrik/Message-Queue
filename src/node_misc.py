my_example_message_content = [
        ("This is a test: hello").encode("UTF-8"),
        ("This is a test: how are you?").encode("UTF-8"),
        ("This is a test: i'm fine, how are you?").encode("UTF-8"),
        ("This is a test: i need to tell you something important").encode("UTF-8"),
        ("This is a test: tell me!").encode("UTF-8")
    ]


def get_example_message_content():
    global my_example_message_content

    return my_example_message_content.pop(0)


def has_message():
    global my_example_message_content

    return len(my_example_message_content) == 0
    
