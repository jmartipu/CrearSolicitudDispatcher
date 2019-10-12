from time import sleep
import datetime
import Settings
from SQSConnection import SQSConnection
from threading import Thread


def dispatch(msg, tipo):
    if tipo == 'Cypress':
        sqs_connection_out = SQSConnection(Settings.AWS_QUEUE_URL_OUT_CYPRESS)
    elif tipo == 'Puppeteer':
        sqs_connection_out = SQSConnection(Settings.AWS_QUEUE_URL_OUT_CYPRESS)
    elif tipo == 'Adb':
        sqs_connection_out = SQSConnection(Settings.AWS_QUEUE_URL_OUT_CYPRESS)
    else:
        sqs_connection_out = SQSConnection(Settings.AWS_QUEUE_URL_OUT_CYPRESS)

    print('Despachando')
    sqs_connection_out.send(msg)
    print('Despachado')


def process():
    try:
        sqs_connection_in = SQSConnection(Settings.AWS_QUEUE_URL_IN)

        with sqs_connection_in:
            sqs_connection_in.receive()
            if sqs_connection_in.message is not '':
                message_attributes = sqs_connection_in.message.get('MessageAttributes')
                message_tipo = message_attributes.get('Tipo').get('StringValue')
                message_body = sqs_connection_in.message.get('Body')
                dispatch(message_body, message_tipo)
                sqs_connection_in.delete()
                
    except Exception as e:
        print(e)


if __name__ == '__main__':
    while True:
        Thread(target=process).start()
        st = str(datetime.datetime.now())
        print(st + ' : alive')
        sleep(Settings.SLEEP_TIME)
