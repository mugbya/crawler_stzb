

def text_handler(text):
    if not isinstance(text, str):
        text = str(text)
    return text.replace('\r', '').replace('\n', '')
