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
package com.datatorrent.contrib.hbase;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.AttributeMap.Attribute;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Stats.OperatorStats.CustomStats;

public class HBaseCsvConfigPutOperatorTest {
	private static final Logger logger = LoggerFactory
			.getLogger(HBasePutOperatorTest.class);

	@Test
	public void testPut() {
		try {
			HBaseTestHelper.clearHBase();
			HBaseCsvConfigPutOperator propPutOperator = new HBaseCsvConfigPutOperator();

			propPutOperator.setTableName("table1");
			propPutOperator.setZookeeperQuorum("127.0.0.1");
			propPutOperator.setZookeeperClientPort(2181);
			String s = "name=milind,st=patrick,ct=fremont,sa=cali";
			propPutOperator
					.setPropFilepath("/home/cloudera/employee.properties");
			propPutOperator.setup(new OperatorContext() {

				@Override
				public <T> T getValue(Attribute<T> key) {
					return null;
				}

				@Override
				public AttributeMap getAttributes() {
					return null;
				}

				@Override
				public void setCustomStats(CustomStats stats) {
				}

				@Override
				public int getId() {
					// TODO Auto-generated method stub
					return 0;
				}
			});
			propPutOperator.beginWindow(0);
			propPutOperator.inputPort.process(s);
			propPutOperator.endWindow();
			HBaseTuple tuple;

			tuple = HBaseTestHelper
					.getHBaseTuple("milind", "colfam0", "street");

			Assert.assertNotNull("Tuple", tuple);
			Assert.assertEquals("Tuple row", tuple.getRow(), "milind");
			Assert.assertEquals("Tuple column family", tuple.getColFamily(),
					"colfam0");
			Assert.assertEquals("Tuple column name", tuple.getColName(),
					"street");
			Assert.assertEquals("Tuple column value", tuple.getColValue(),
					"patrick");
		} catch (IOException e) {

			logger.error(e.getMessage());
		}
	}
}