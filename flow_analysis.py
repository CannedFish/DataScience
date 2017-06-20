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
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.mllib.linalg import Vectors

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
            data[row.row[-2:]].append(float(row.columns['exp:total'].value))
    transport.close()

    return data

def prepare_data(raw):

    def gen_label(r):
        p = {}
        for key in r.iterkeys():
            p[key] = [(l, Vectors.dense(r[key][s:s+window_size])) \
                    for s, l in \
                    zip(range(len(r[key])-window_size), r[key][window_size:])]
        return p

    train_len = int(len(raw['00']) / 3 * 2)
    train = {}
    test = {}
    for key in raw.iterkeys():
        train[key] = raw[key][:train_len]
        test[key] = raw[key][train_len:]
    train = gen_label(train)
    test = gen_label(test)

    return train, test

def main():
    sc = SparkContext(appName='FlowPrediction')
    sqlContext = SQLContext(sc)

    train, test = prepare_data(get_raw_data())
    for key in train.iterkeys():
        # TODO: Use cross validation to tune parameters
        # train
        train_df = sqlContext.createDataFrame(train[key], ['label', 'features'])
        lr = LinearRegression(maxIter=30, regParam=0.3, elasticNetParam=0.8)
        model = lr.fit(train_df)
        # test
        test_df = sqlContext.createDataFrame(test[key], ['label', 'features'])
        evaluator = RegressionEvaluator()
        print("%s: %f" % (key, evaluator.evaluate(model.transform(test_df))))
        model.save("FlowAnalysis/model_%s" % key)

    sc.stop()

if __name__ == '__main__':
    main()

