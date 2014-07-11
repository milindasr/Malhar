package com.datatorrent.contrib.accumulo;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.contrib.accumulo.AccumuloOutputOperatorTest.TestAccumuloOutputOperator;

public class AccumuloApp implements StreamingApplication{

	@Override
	public void populateDAG(DAG dag, Configuration conf) {
		AccumuloTestHelper.getConnector();
		  AccumuloTestHelper.clearTable();
		  
		dag.setAttribute(DAG.APPLICATION_NAME, "AccumuloOutputTest");
	      AccumuloRowTupleGenerator rtg = dag.addOperator("tuplegenerator", AccumuloRowTupleGenerator.class);
	      TestAccumuloOutputOperator taop = dag.addOperator("testhbaseput", TestAccumuloOutputOperator.class);
	      dag.addStream("ss", rtg.outputPort, taop.input);

	      taop.getStore().setTableName("tab1");
	      taop.getStore().setZookeeperHost("127.0.0.1");
	      taop.getStore().setInstanceName("milind");
	      taop.getStore().setUserName("root");
	      taop.getStore().setPassword("milind");
		
	}

}
