package com.datatorrent.contrib.accumulo;

import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAGContext;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.contrib.accumulo.AbstractAccumuloTransactionalOutputOperator;
public class AccumuloApp implements StreamingApplication {

	@Override
	public void populateDAG(DAG dag, Configuration conf) {
		AccumuloTestHelper.getConnector();
		AccumuloTestHelper.clearTable();

		dag.setAttribute(DAG.APPLICATION_NAME, "AccumuloOutputTest");
		AccumuloRowTupleGenerator rtg = dag.addOperator("tuplegenerator",
				AccumuloRowTupleGenerator.class);
		TestAccumuloOutputOperator taop = dag.addOperator(
				"testaccumulooperator", TestAccumuloOutputOperator.class);
		dag.addStream("ss", rtg.outputPort, taop.input);
		AttributeMap attributes = dag.getAttributes();
	    attributes.put(DAGContext.CONTAINER_MEMORY_MB, 4096);
		taop.getStore().setTableName("tab1");
		taop.getStore().setZookeeperHost("127.0.0.1");
		taop.getStore().setInstanceName("milind");
		taop.getStore().setUserName("root");
		taop.getStore().setPassword("milind");

	}
	
	public static class TestAccumuloOutputOperator extends AbstractAccumuloTransactionalOutputOperator<AccumuloTuple> {

		@Override
		public Mutation operationMutation(AccumuloTuple t) {
		Mutation mutation = new Mutation(t.getRow().getBytes());
		mutation.put(t.getColFamily().getBytes(),t.getColName().getBytes(),t.getColValue().getBytes());
		return mutation;
		}

		  }
}