import time
from time import gmtime, strftime
import socket
import random

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(('127.0.0.1', 20519))

while True:
    for i in range(0, 9):
        collectd_value = random.randint(0, 100)
        collectd_alea = "{'host':'malavika" + str(i) + "','collectd_type':'cpu','value':'" + \
                    str(collectd_value) + "','timestamp':'" + \
                    strftime("%Y-%m-%dT%H:%M:%S.000Z", gmtime()) + "'}\n"

        sock.send(collectd_alea)
        print collectd_alea
        time.sleep(1)

        collectd_value = random.randint(0, 4000000)
        collectd_alea = "{'host':'malavika" + str(i) + "','collectd_type':'memory','value':'" + \
            str(collectd_value) + "','timestamp':'" + \
            strftime("%Y-%m-%dT%H:%M:%S.000Z", gmtime()) + "'}\n"

        sock.send(collectd_alea)
        print collectd_alea
        time.sleep(1)

    time.sleep(5)


