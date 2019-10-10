from time import sleep
import datetime
import Settings
from SQSConnection import SQSConnection
from threading import Thread

def dispatch(msg):
    sqs_connection_out = SQSConnection(Settings.AWS_QUEUE_URL_OUT)
    print('Despachando')
    sqs_connection_out.send(msg)
    print('Despachado')


def process():
    try:
        sqs_connection_in = SQSConnection(Settings.AWS_QUEUE_URL_IN)

        with sqs_connection_in:
            sqs_connection_in.receive()
            if sqs_connection_in.message is not '':
                message_body = sqs_connection_in.message.get('MessageBody')
                dispatch(message_body)
                sqs_connection_in.delete()
                
    except Exception as e:
        print(e)


if __name__ == '__main__':
    while True:
        Thread(target=process).start()
        st = str(datetime.datetime.now())
        print(st + ' : alive')
        sleep(Settings.SLEEP_TIME)
