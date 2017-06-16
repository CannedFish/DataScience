# -*- coding: utf-8 -*-

from __future__ import print_function

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase import Hbase
from hbase.ttypes import Mutation, BatchMutation

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.ml.regression import GBTRegressor

# Parameters:
window_size = 10

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

def get_raw_data():
    table = 'ExpressStatistics'
    client, transport = get_client(table, '192.168.1.81', 9090)
    transport.open()
    scanner = client.scannerOpenWithPrefix(table, '2010', ['exp:total'], {})
    data = {'00': [], '01': [], '02': [], '03': [], '04': []}
    while True:
        rows = client.scannerGetList(scanner, 10000)
        if len(rows) == 0:
            break
        for row in rows:
            data[row.row[-2:]].append(row.columns['exp:total'].value)
    transport.close()
    return data

def prepare_data(raw):
    train, test = [], []
    return train, test

def main():
    sc = SparkContext(appName='FlowPrediction')
    sqlContext = SQLContext(sc)

    train, test = prepare_data(get_raw_data())
    train_df = sqlContext.createDataFrame(train, ['label', 'features'])
    gbt = GBTRegressor(maxIter=5, maxDepth=2)
    model = gbt.fit(train_df)

if __name__ == '__main__':
    main()

