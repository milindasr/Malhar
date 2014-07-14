/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.accumulo;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.annotation.ShipContainingJars;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.db.AbstractAggregateTransactionableStoreOutputOperator;

/**
 * Operator for storing tuples in Accumulo rows.<br>
 * 
 * <br>
 * This class provides a Accumulo output operator that can be used to store
 * tuples in rows in a Accumulo table. It should be extended to provide specific
 * implementation. The extending class should implement operationMutation method
 * and provide a Accumulo Mutation metric object that specifies where and what
 * to store for the tuple in the table.<br>
 * 
 * <br>
 * This class provides a batch put where tuples are collected till the end
 * window and they are put on end window
 * 
 * Note that since Accumulo doesn't support transactions this store cannot
 * guarantee each tuple is written only once to Accumulo in case the operator is
 * restarted from an earlier checkpoint. It only tries to minimize the number of
 * duplicates limiting it to the tuples that were processed in the window when
 * the operator shutdown.
 * 
 * @param <T>
 *            The tuple type
 */
@ShipContainingJars(classes = {
		org.apache.accumulo.core.client.Connector.class,
		org.apache.hadoop.hbase.util.Bytes.class,
		org.apache.accumulo.fate.util.Daemon.class,
		org.apache.accumulo.start.classloader.AccumuloClassLoader.class,
		org.apache.accumulo.trace.instrument.CountSampler.class,
		org.apache.thrift.protocol.TField.class })
public abstract class AbstractAccumuloTransactionalOutputOperator<T>
		extends
		AbstractAggregateTransactionableStoreOutputOperator<T, AccumuloTransactionalStore> {
	private static final transient Logger logger = LoggerFactory
			.getLogger(AbstractAccumuloTransactionalOutputOperator.class);
	
	public AbstractAccumuloTransactionalOutputOperator() {
		store = new AccumuloTransactionalStore();
	}
	@Override
	public void storeAggregate() {
		try {
			store.getBatchwriter().flush();
		} catch (MutationsRejectedException e) {
			logger.error("unable to write mutations during end window", e);
			DTThrowable.rethrow(e);
		}
	}
	/**
	 * 
	 * @param t
	 * @return Mutation
	 */
	public abstract Mutation operationMutation(T t);

	@Override
	public void processTuple(T tuple) {
		Mutation mutation = operationMutation(tuple);
		try {
			store.getBatchwriter().addMutation(mutation);
		} catch (MutationsRejectedException e) {
			logger.error("unable to write mutations", e);
			DTThrowable.rethrow(e);
		}
	}
}
