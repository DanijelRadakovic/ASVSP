#!/usr/bin/python3

import os
import sys
import time
import random
import re
import datetime
from kafka import KafkaProducer
import kafka.errors

KAFKA_BROKER = os.environ['KAFKA_BROKER']
TOPIC = 'apache-access-log'

ERROR_PERIOD = 2


def update_log(line):
    tokens = list(map(''.join, re.findall(r'\"(.*?)\"|\[(.*?)\]|(\S+)', line)))
    tokens[3] = datetime.datetime.now().strftime('[%d/%b/%Y:%H:%M:%S %z]')
    return ' '.join(tokens)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Required path to data directory', file=sys.stderr)
        sys.exit(-1)

    DATA_PATH = sys.argv[1]

    while True:
        try:
            producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER.split(','))
            print('Connected to Kafka!')
            break
        except kafka.errors.NoBrokersAvailable as e:
            print(e)
            time.sleep(3)

    log_names = [os.path.join(DATA_PATH, file_name) for file_name in os.listdir(DATA_PATH)]
    log_files = []
    error_log_files = []

    for log_name in log_names:
        try:
            if 'error' in log_name:
                error_log_files.append(open(log_name, 'r'))
            else:
                log_files.append(open(log_name, 'r'))
        except IOError:
            continue

    error_period_cnt = 0

    while len(log_files) > 0 or len(error_log_files) > 0:
        if len(log_files) == 0 or (error_period_cnt == ERROR_PERIOD and len(error_log_files) > 0):  # read error logs
            file = random.choice(error_log_files)
            print('===== Reading from file ', file.name, ' =====')
            for log in file:
                producer.send(TOPIC, key=bytes('apache-access-log', 'utf-8'), value=bytes(update_log(log), 'utf-8'))
                time.sleep(1)
            error_log_files.remove(file)
            file.close()
            error_period_cnt = 0
        else:
            error_period_cnt += 1
            file = random.choice(log_files)
            lines = random.randint(1, 8)
            print('===== Reading ', lines, ' from file ', file.name, ' =====')
            for i in range(lines):
                log = file.readline()
                if log == '':
                    log_files.remove(file)
                    file.close()
                    break
                try:
                    producer.send(TOPIC, key=bytes('apache-access-log', 'utf-8'), value=bytes(update_log(log), 'utf-8'))
                    time.sleep(1)
                except IndexError:
                    pass
