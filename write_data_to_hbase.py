# -*- coding: utf-8 -*-

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase import Hbase
from hbase.ttypes import Mutation, BatchMutation

from optparse import OptionParser
import random

def get_client(table, netloc, port):
    """
    @Return
        client, transport
    """
    transport = TTransport.TBufferedTransport(
            TSocket.TSocket(netloc, port))
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = Hbase.Client(protocol)

    return client, transport

md = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

def get_dates(start, months):
    year, month, day = map(int, start.split('/'))
    for m in xrange(months):
        days = md[month-1]
        for d in xrange(day, days+1):
            yield '%d/%02d/%02d' % (year, month, d)
        day = 1
        if month == 12:
            month = 1
            year += 1
        else:
            month += 1

PROVIDERIDs = [
    ('STO', '00'), 
    ('YTO', '01'), 
    ('ZTO', '02'), 
    ('BEST', '03'), 
    ('YUNDA', '04')
]

# use mutateRows to put data to HBase
def input_to_es(table, ops):
    client, transport = get_client(table, ops.location, ops.port)
    transport.open()
    rows = []
    print 'Begin to write date to table, from %s to %d months later' \
            % (ops.start, int(ops.months))
    for date in get_dates(ops.start, int(ops.months)):
        for provider, no in PROVIDERIDs:
            rowkey = date.replace('/', '') + no
            provider_id = Mutation(column='info:logisticproviderid', value=provider)
            et = random.randint(100000, 99999999)
            exp_total = Mutation(column='exp:total', value=str(et))
            exp_error = Mutation(column='exp:error', \
                    value=str(random.randint(100, 999)))
            expstate_total = Mutation(column='expstate:total', value=str(et*10))
            expstate_error = Mutation(column='expstate:error', \
                    value=str(random.randint(100, 999)))
            rows.append(BatchMutation(rowkey, [provider_id, exp_total, \
                    exp_error, expstate_total, expstate_error]))
    client.mutateRows(table, rows, {})
    transport.close()
    print 'Write to ExpressStatistics finished'

def main():
    usage = 'Usage: %prog [options] $table_name'
    parser = OptionParser(usage=usage)
    parser.add_option('-l', '--location', dest='location', default="192.168.1.81", \
            help="The network location of HBase Thrift server")
    parser.add_option('-p', '--port', dest='port', default=9090, \
            help="The port of HBase Thrift server")
    parser.add_option('-s', '--start', dest='start', default="2010/01/01", \
            help="The date of data starting to insert")
    parser.add_option('-m', '--months', dest='months', default=6, \
            help="How many months of data to insert")
    options, args = parser.parse_args()

    if len(args) != 1:
        parser.error("Incorrent number of arguments")

    if args[0] == 'ExpressStatistics':
        input_to_es(args[0], options)
    else:
        parser.error("Unknown table")

if __name__ == '__main__':
    main()

