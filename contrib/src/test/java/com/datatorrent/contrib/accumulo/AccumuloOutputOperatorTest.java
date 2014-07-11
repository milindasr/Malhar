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
package com.datatorrent.contrib.accumulo;

import junit.framework.Assert;

import org.apache.accumulo.core.data.Mutation;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;

/**
 *
 */
public class AccumuloOutputOperatorTest
{
  private static final Logger logger = LoggerFactory.getLogger(AccumuloOutputOperatorTest.class);

  @Test
  public void testPut()
  {
	  AccumuloTestHelper.getConnector();
	  AccumuloTestHelper.clearTable();
	  
      LocalMode lma = LocalMode.newInstance();
      DAG dag = lma.getDAG();

      dag.setAttribute(DAG.APPLICATION_NAME, "AccumuloOutputTest");
      AccumuloRowTupleGenerator rtg = dag.addOperator("tuplegenerator", AccumuloRowTupleGenerator.class);
      TestAccumuloOutputOperator taop = dag.addOperator("testhbaseput", TestAccumuloOutputOperator.class);
      dag.addStream("ss", rtg.outputPort, taop.input);

      taop.getStore().setTableName("tab1");
      taop.getStore().setZookeeperHost("127.0.0.1");
      taop.getStore().setInstanceName("milind");
      taop.getStore().setUserName("root");
      taop.getStore().setPassword("milind");

      LocalMode.Controller lc = lma.getController();
      lc.setHeartbeatMonitoringEnabled(false);
      lc.run(20000);
     
      AccumuloTuple tuple = AccumuloTestHelper.getAccumuloTuple("row0", "colfam0", "col-0");
      Assert.assertNotNull("Tuple", tuple);
      Assert.assertEquals("Tuple row", tuple.getRow(), "row0");
      Assert.assertEquals("Tuple column family", tuple.getColFamily(), "colfam0");
      Assert.assertEquals("Tuple column name", tuple.getColName(), "col-0");
      Assert.assertEquals("Tuple column value", tuple.getColValue(), "val-0-0");
      tuple = AccumuloTestHelper.getAccumuloTuple("row499", "colfam0", "col-0");
      Assert.assertNotNull("Tuple", tuple);
      Assert.assertEquals("Tuple row", tuple.getRow(), "row499");
      Assert.assertEquals("Tuple column family", tuple.getColFamily(), "colfam0");
      Assert.assertEquals("Tuple column name", tuple.getColName(), "col-0");
      Assert.assertEquals("Tuple column value", tuple.getColValue(), "val-499-0");
    

       
  }
 

  public static class TestAccumuloOutputOperator extends AbstractAccumuloTransactionableOutputOperator<AccumuloTuple> {

	@Override
	public Mutation operationMutation(AccumuloTuple t) {
		 Mutation mutation = new Mutation(t.getRow().getBytes());
	      mutation.put(t.getColFamily().getBytes(),t.getColName().getBytes(),t.getColValue().getBytes());
	      return mutation;
	}

  }
}
