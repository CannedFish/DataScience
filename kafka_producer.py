# -*- coding: utf-8 -*-

from __future__ import print_function

from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError

import json
import random
import time
from optparse import OptionParser

def message_gen():
    no = 1
    pid = ['STO', 'YTO', 'ZTO', 'BEST', 'YUNDA']
    city = ['BJ', 'NJ', 'LZ', 'SZ', 'SH']
    name = ['Alice', 'Bob', 'Crystal', 'Dog', 'Emma']
    content = ['Food', 'Other']
    while True:
        now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        exp = {
            "id": "2017060601035#%d" % no,
            "data": {
                "info": {
                    "logisticproviderid": random.choice(pid),
                    "mailno": str(random.randint(1000000, 9999999)),
                    "mailtype": str(random.randint(1, 4)),
                    "weight": str(random.randint(1, 40)),
                    "sencitycode": random.choice(city),
                    "reccitycode": random.choice(city),
                    "senareacode": "",
                    "recareacode": "",
                    "inserttime": time.time()
                },
                "send": {
                    "senname": random.choice(name),
                    "senmobile": "139123456",
                    "senphone": "86123456",
                    "senprov": str(random.randint(1, 40)),
                    "sencity": str(random.randint(1, 40)),
                    "sencounty": str(random.randint(1, 40)),
                    "senaddress": "Road abc"
                },
                "recv": {
                    "recname": random.choice(name),
                    "recmobile": "139654321",
                    "recphone": "86654321",
                    "recprov": str(random.randint(1, 40)),
                    "reccity": str(random.randint(1, 40)),
                    "reccounty": str(random.randint(1, 40)),
                    "recaddress": "Road def"
                },
                "pkg": {
                    "typeofcontents": random.choice(content),
                    "nameofcoutents": "Something",
                    "mailcode": random.choice(['Y', 'N']),
                    "recdatetime": now,
                    "insurancevalue": str(random.randint(100, 400))
                }
            },
            "type": "ExpressContract",
            "usage": random.choice(["upload", "re-upload"])
        }
        yield exp
        no += 1

def run_producer(opt):
    producer = KafkaProducer(\
            bootstrap_servers=['%s:%s' % (opt.host, opt.port)], \
            value_serializer=lambda m: json.dumps(m).encode('utf-8'))
    print("Sending message to %s:%s, topic is %s." % \
            (opt.host, opt.port, opt.topic))
    for msg in message_gen():
        producer.send(opt.topic, msg)
        producer.flush()
        time.sleep(float(opt.interval))

def run_consumer(opt):
    consumer = KafkaConsumer(\
            opt.topic, \
            group_id=opt.groupid, \
            bootstrap_servers=['%s:%s' % (opt.host, opt.port)], \
            value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    for message in consumer:
        print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, \
                message.offset, message.key, message.value))

if __name__ == '__main__':
    usage = 'Usage: %prog [options] ${producer|consumer}'
    parser = OptionParser(usage=usage)
    parser.add_option('-H', '--Host', dest='host', default='127.0.0.1', \
            help="The host of Kafka broker")
    parser.add_option('-p', '--port', dest='port', default='9092', \
            help="The port of Kafka broker")
    parser.add_option('-t', '--topic', dest='topic', default='default', \
            help="The topic to produce or consume messages")
    parser.add_option('-g', '--groupid', dest='groupid', default='default-group', \
            help="The group ID of consumer")
    parser.add_option('-i', '--interval', dest='interval', default=1, \
            help="Seconds between two messages produced")
    options, args = parser.parse_args()

    if len(args) != 1:
        parser.error("Incorrect number of arguments.")

    if args[0] == 'producer':
        run_producer(options)
    elif args[0] == 'consumer':
        run_consumer(options)
    else:
        parser.error("Unknown role, only producer or consumer.")

