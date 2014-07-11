/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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

import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.db.TransactionableStore;

/**
 * Provides transactional support.Not intended for true transactional
 * properties. It deos not guarantee exactly once property.It only skips tuple
 * processed in previous windows
 */
public class AccumuloTransactionalStore extends AccumuloStore implements
		TransactionableStore {
	private static final transient Logger logger = LoggerFactory
			.getLogger(AccumuloTransactionalStore.class);
	private static final String DEFAULT_ROW_NAME = "HBaseOperator_row";
	private static final String DEFAULT_COLUMN_FAMILY_NAME = "HBaseOutputOperator_cf";
	private static final String DEFAULT_LAST_WINDOW_PREFIX_COLUMN_NAME = "last_window";
	private transient String rowName;
	private transient String columnFamilyName;

	private transient byte[] rowBytes;
	private transient byte[] columnFamilyBytes;

	private transient String lastWindowColumnName;
	private transient byte[] lastWindowColumnBytes;

	public AccumuloTransactionalStore() {
		rowName = DEFAULT_ROW_NAME;
		columnFamilyName = DEFAULT_COLUMN_FAMILY_NAME;
		lastWindowColumnName = DEFAULT_LAST_WINDOW_PREFIX_COLUMN_NAME;
		constructKeys();
	}

	/**
	 * the values are stored as byte arrays.This method converts string to byte
	 * arrays. uses util class in hbase library to do so.
	 */
	private void constructKeys() {
		rowBytes = Bytes.toBytes(rowName);
		columnFamilyBytes = Bytes.toBytes(columnFamilyName);
	}

	public String getRowName() {
		return rowName;
	}

	public void setRowName(String rowName) {
		this.rowName = rowName;
		constructKeys();
	}

	public String getColumnFamilyName() {
		return columnFamilyName;
	}

	public void setColumnFamilyName(String columnFamilyName) {
		this.columnFamilyName = columnFamilyName;
		constructKeys();
	}

	@Override
	public void beginTransaction() {
		// accumulo does not support transactions
	}

	@Override
	public void commitTransaction() {
		// accumulo does not support transactions

	}

	@Override
	public void rollbackTransaction() {
		// accumulo does not support transactions

	}

	@Override
	public boolean isInTransaction() {
		// accumulo does not support transactions
		return false;
	}

	@Override
	public long getCommittedWindowId(String appId, int operatorId) {
		byte[] value = null;
		Authorizations auths = new Authorizations();
		Scanner scan = null;
		String columnKey = appId + "_" + operatorId + "_"
				+ lastWindowColumnName;
		lastWindowColumnBytes = Bytes.toBytes(columnKey);
		try {
			scan = connector.createScanner(tableName, auths);
		} catch (TableNotFoundException e) {
			logger.error("error getting committed window id", e);
			DTThrowable.rethrow(e);
		}

		scan.setRange(new Range(new Text(rowBytes)));
		scan.fetchColumn(new Text(columnFamilyBytes), new Text(
				lastWindowColumnBytes));
		for (Entry<Key, Value> entry : scan) {
			value = entry.getValue().get();
		}
		if (value != null) {
			long longval = Bytes.toLong(value);
			return longval;
		}
		return -1;

	}

	@Override
	public void storeCommittedWindowId(String appId, int operatorId,
			long windowId) {
		BatchWriter bw = null;
		byte[] WindowIdBytes = Bytes.toBytes(windowId);
		String columnKey = appId + "_" + operatorId + "_"
				+ lastWindowColumnName;
		lastWindowColumnBytes = Bytes.toBytes(columnKey);
		Mutation mutation = new Mutation(rowBytes);
		mutation.put(columnFamilyBytes, lastWindowColumnBytes, WindowIdBytes);
		BatchWriterConfig config = new BatchWriterConfig();
		try {
			bw = connector.createBatchWriter(tableName, config);
		} catch (TableNotFoundException e) {
			logger.error("error storing committed window id", e);
			DTThrowable.rethrow(e);
		}
		try {
			bw.addMutation(mutation);
			bw.close();
		} catch (MutationsRejectedException e) {
			logger.error("error storing committed window id", e);
			DTThrowable.rethrow(e);
		}

	}

	@Override
	public void removeCommittedWindowId(String appId, int operatorId) {
		// accumulo does not support transactions

	}

}
