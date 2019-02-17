import websocket
import json
import time

try:
    import thread
except ImportError:
    import _thread as thread


def on_message(ws, message):
    print(message)
    data = json.loads(message)
    content = data['content']

    if data['type'] == 'MESSAGE':
        print("process and publish message")
        time.sleep(5)
        content['processed'] = True

        message_out = {
            'type': 'COMMAND',
            'headers': {
                'command': 'PUBLISH',
                'topic': 'events'
            },
            'content': content
        }
        json_message_out = json.dumps(message_out)
        ws.send(json_message_out)


def on_error(ws, error):
    print(error)


def on_close(ws):
    print("close")


def on_open(ws):
    print("open")
    data = {
        'type': 'COMMAND',
        'headers': {
            'command': 'SUBSCRIBE',
            'topic': 'events'
        },
        'content': {
            'filter': {
                'processed': False
            }
        }
    }

    json_data = json.dumps(data, ensure_ascii=False)
    ws.send(json_data)


if __name__ == "__main__":
    ws = websocket.WebSocketApp("ws://localhost:8080/ws",
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()
