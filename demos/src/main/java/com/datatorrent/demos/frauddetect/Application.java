/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.frauddetect;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAGContext;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.demos.frauddetect.operator.HdfsStringOutputOperator;
import com.datatorrent.demos.frauddetect.operator.MongoDBOutputOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.PubSubWebSocketInputOperator;
import com.datatorrent.lib.io.PubSubWebSocketOutputOperator;
import com.datatorrent.lib.math.RangeKeyVal;
import com.datatorrent.lib.multiwindow.SimpleMovingAverage;
import com.datatorrent.lib.util.KeyValPair;

/**
 * Fraud detection application
 *
 * @since 0.9.0
 */
@ApplicationAnnotation(name="FraudDetectDemo")
public class Application implements StreamingApplication
{
  private static final Logger LOG = LoggerFactory.getLogger(Application.class);
  protected int appWindowCount = 1; // 1 seconds
  protected int aggrWindowCount = 1; // 1 seconds
  protected int amountSamplerWindowCount = 1; // 30 seconds
  protected int binSamplerWindowCount = 1; // 30 seconds
  public static final String MONGO_HOST_PROPERTY = "demo.frauddetect.mongo.host";
  public static final String MONGO_DATABASE_PROPERTY = "demo.frauddetect.mongo.db";
  public static final String MONGO_USER_PROPERTY = "demo.frauddetect.mongo.user";
  public static final String MONGO_PASSWORD_PROPERTY = "demo.frauddetect.mongo.password";

  public MerchantTransactionGenerator getMerchantTransactionGenerator(String name, DAG dag)
  {
    MerchantTransactionGenerator oper = dag.addOperator(name, MerchantTransactionGenerator.class);
    return oper;
  }

  public PubSubWebSocketInputOperator getPubSubWebSocketInputOperator(String name, DAG dag, URI duri, String topic) throws Exception
  {
    PubSubWebSocketInputOperator reqin = dag.addOperator(name, new PubSubWebSocketInputOperator());
    reqin.setUri(duri);
    reqin.addTopic(topic);
    return reqin;
  }

  public PubSubWebSocketOutputOperator getPubSubWebSocketOutputOperator(String name, DAG dag, URI duri, String topic) throws Exception
  {
    PubSubWebSocketOutputOperator out = dag.addOperator(name, new PubSubWebSocketOutputOperator());
    out.setUri(duri);
    return out;
  }

  public MerchantTransactionInputHandler getMerchantTransactionInputHandler(String name, DAG dag)
  {
    MerchantTransactionInputHandler oper = dag.addOperator(name, new MerchantTransactionInputHandler());
    return oper;
  }

  public BankIdNumberSamplerOperator getBankIdNumberSamplerOperator(String name, DAG dag, Configuration conf)
  {
    BankIdNumberSamplerOperator oper = dag.addOperator(name, BankIdNumberSamplerOperator.class);
    return oper;
  }

  public MerchantTransactionBucketOperator getMerchantTransactionBucketOperator(String name, DAG dag)
  {
    MerchantTransactionBucketOperator oper = dag.addOperator(name, MerchantTransactionBucketOperator.class);
    return oper;
  }

  public RangeKeyVal<MerchantKey, Long> getRangeKeyValOperator(String name, DAG dag)
  {
    RangeKeyVal oper = dag.addOperator(name, new RangeKeyVal<MerchantKey, Long>());
    return oper;
  }

  public SimpleMovingAverage<MerchantKey, Long> getSimpleMovingAverageOpertor(String name, DAG dag)
  {
    SimpleMovingAverage<MerchantKey, Long> oper = dag.addOperator(name, SimpleMovingAverage.class);
    return oper;
  }

  public SlidingWindowSumKeyVal<KeyValPair<MerchantKey, String>, Integer> getSlidingWindowSumOperator(String name, DAG dag)
  {
    SlidingWindowSumKeyVal<KeyValPair<MerchantKey, String>, Integer> oper = dag.addOperator(name, SlidingWindowSumKeyVal.class);
    return oper;
  }

  public AverageAlertingOperator getAverageAlertingOperator(String name, DAG dag, Configuration conf)
  {
    AverageAlertingOperator oper = dag.addOperator(name, AverageAlertingOperator.class);
    return oper;
  }

  public CreditCardAmountSamplerOperator getTransactionAmountSamplerOperator(String name, DAG dag, Configuration conf)
  {
    CreditCardAmountSamplerOperator oper = dag.addOperator(name, CreditCardAmountSamplerOperator.class);
    return oper;
  }

  public TransactionStatsAggregator getTransactionStatsAggregator(String name, DAG dag)
  {
    TransactionStatsAggregator oper = dag.addOperator(name, TransactionStatsAggregator.class);
    return oper;
  }

  public MongoDBOutputOperator getMongoDBOutputOperator(String name, DAG dag, String collection, Configuration conf)
  {
    MongoDBOutputOperator oper = dag.addOperator(name, MongoDBOutputOperator.class);
    // oper.setUserName("fraudadmin");
    // oper.setPassWord("1234");
    oper.setCollection(collection);

    return oper;
  }

  public HdfsStringOutputOperator getHdfsOutputOperator(String name, DAG dag, String folderName)
  {
    HdfsStringOutputOperator oper = dag.addOperator("hdfs", HdfsStringOutputOperator.class);
    oper.setFilePath(folderName + "/%(contextId)/transactions.out.part%(partIndex)");
    oper.setBytesPerFile(1024 * 1024 * 1024);
    return oper;
  }

  public ConsoleOutputOperator getConsoleOperator(String name, DAG dag, String prefix, String format)
  {
    ConsoleOutputOperator oper = dag.addOperator(name, ConsoleOutputOperator.class);
    // oper.setStringFormat(prefix + ": " + format);
    return oper;
  }

  /**
   * Create the DAG
   */
  @SuppressWarnings("unchecked")
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    try {
      String gatewayAddress = dag.getValue(DAGContext.GATEWAY_CONNECT_ADDRESS);
      if (gatewayAddress == null) {
        gatewayAddress = "localhost:9090";
      }
      URI duri = URI.create("ws://" + gatewayAddress + "/pubsub");

      dag.setAttribute(DAG.APPLICATION_NAME, "FraudDetectionApplication");
      dag.setAttribute(DAG.DEBUG, false);
    //  dag.setAttribute(DAG.STREAMING_WINDOW_SIZE_MILLIS, 1000);

      PubSubWebSocketInputOperator userTxWsInput = getPubSubWebSocketInputOperator("userTxInput", dag, duri, "demos.app.frauddetect.submitTransaction");
      PubSubWebSocketOutputOperator ccUserAlertWsOutput = getPubSubWebSocketOutputOperator("ccUserAlertQueryOutput", dag, duri, "demos.app.frauddetect.fraudAlert");
      PubSubWebSocketOutputOperator avgUserAlertwsOutput = getPubSubWebSocketOutputOperator("avgUserAlertQueryOutput", dag, duri, "demos.app.frauddetect.fraudAlert");
      PubSubWebSocketOutputOperator binUserAlertwsOutput = getPubSubWebSocketOutputOperator("binUserAlertOutput", dag, duri, "demos.app.frauddetect.fraudAlert");
      PubSubWebSocketOutputOperator txSummaryWsOutput = getPubSubWebSocketOutputOperator("txSummaryWsOutput", dag, duri, "demos.app.frauddetect.txSummary");

      SlidingWindowSumKeyVal<KeyValPair<MerchantKey, String>, Integer> smsOperator = getSlidingWindowSumOperator("movingSum", dag);
      MerchantTransactionGenerator txReceiver = getMerchantTransactionGenerator("txReceiver", dag);
      MerchantTransactionInputHandler txInputHandler = getMerchantTransactionInputHandler("txInputHandler", dag);
      BankIdNumberSamplerOperator binSampler = getBankIdNumberSamplerOperator("bankInfoFraudDetector", dag, conf);
      MerchantTransactionBucketOperator txBucketOperator = getMerchantTransactionBucketOperator("txFilter", dag);
      RangeKeyVal<MerchantKey, Long> rangeOperator = getRangeKeyValOperator("rangePerMerchant", dag);
      SimpleMovingAverage<MerchantKey, Long> smaOperator = getSimpleMovingAverageOpertor("smaPerMerchant", dag);
      TransactionStatsAggregator txStatsAggregator = getTransactionStatsAggregator("txStatsAggregator", dag);
      AverageAlertingOperator avgAlertingOperator = getAverageAlertingOperator("avgAlerter", dag, conf);
      CreditCardAmountSamplerOperator ccSamplerOperator = getTransactionAmountSamplerOperator("amountFraudDetector", dag, conf);
      HdfsStringOutputOperator hdfsOutputOperator = getHdfsOutputOperator("hdfsOutput", dag, "fraud");

      MongoDBOutputOperator mongoTxStatsOperator = getMongoDBOutputOperator("mongoTxStatsOutput", dag, "txStats", conf);
      MongoDBOutputOperator mongoBinAlertsOperator = getMongoDBOutputOperator("mongoBinAlertsOutput", dag, "binAlerts", conf);
      MongoDBOutputOperator mongoCcAlertsOperator = getMongoDBOutputOperator("mongoCcAlertsOutput", dag, "ccAlerts", conf);
      MongoDBOutputOperator mongoAvgAlertsOperator = getMongoDBOutputOperator("mongoAvgAlertsOutput", dag, "avgAlerts", conf);

      dag.addStream("userTxStream", userTxWsInput.outputPort, txInputHandler.userTxInputPort);
      dag.addStream("transactions", txReceiver.txOutputPort, txBucketOperator.inputPort).setLocality(DAG.Locality.CONTAINER_LOCAL);
      dag.addStream("txData", txReceiver.txDataOutputPort, hdfsOutputOperator.input); // dump all tx into Hdfs
      dag.addStream("userTransactions", txInputHandler.txOutputPort, txBucketOperator.txUserInputPort);

      // dag.addStream("bankInfoData", txBucketOperator.binOutputPort, binSampler.txInputPort);
      dag.addStream("bankInfoData", txBucketOperator.binCountOutputPort, smsOperator.data);
      dag.addStream("bankInfoCount", smsOperator.integerSum, binSampler.txCountInputPort);

      dag.addStream("filteredTransactions", txBucketOperator.txOutputPort, rangeOperator.data, smaOperator.data, avgAlertingOperator.txInputPort);
      dag.addStream("creditCardData", txBucketOperator.ccAlertOutputPort, ccSamplerOperator.inputPort);
      dag.addStream("txnSummaryData", txBucketOperator.summaryTxnOutputPort, txSummaryWsOutput.input);

      dag.addStream("smaAlerts", smaOperator.doubleSMA, avgAlertingOperator.smaInputPort);

      dag.addStream("binAlerts", binSampler.countAlertOutputPort, mongoBinAlertsOperator.inputPort);
      dag.addStream("binAlertsNotification", binSampler.countAlertNotificationPort, binUserAlertwsOutput.input);
      dag.addStream("rangeData", rangeOperator.range, txStatsAggregator.rangeInputPort);
      dag.addStream("smaData", smaOperator.longSMA, txStatsAggregator.smaInputPort);

      dag.addStream("txStatsOutput", txStatsAggregator.txDataOutputPort, mongoTxStatsOperator.inputPort);

      dag.addStream("avgAlerts", avgAlertingOperator.avgAlertOutputPort, mongoAvgAlertsOperator.inputPort);
      dag.addStream("avgAlertsNotification", avgAlertingOperator.avgAlertNotificationPort, avgUserAlertwsOutput.input);

      dag.addStream("ccAlerts", ccSamplerOperator.ccAlertOutputPort, mongoCcAlertsOperator.inputPort);
      dag.addStream("ccAlertsNotification", ccSamplerOperator.ccAlertNotificationPort, ccUserAlertWsOutput.input);
      
    } catch (Exception exc) {
      exc.printStackTrace();
    }
  }
}
