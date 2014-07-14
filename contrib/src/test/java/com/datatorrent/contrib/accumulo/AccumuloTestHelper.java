package com.datatorrent.contrib.accumulo;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.contrib.hbase.HBaseTuple;


public class AccumuloTestHelper {
	static Connector con;
	public static final byte[] colfam0_bytes = Bytes.toBytes("colfam0");
	public static final byte[] col0_bytes = Bytes.toBytes("col-0");
	private static final Logger logger = LoggerFactory.getLogger(AccumuloTestHelper.class);
	public static void createTable() {
		TableOperations tableoper = con.tableOperations();
		if (!tableoper.exists("tab1")) {
			try {
				tableoper.create("tab1");
			} catch (Exception e) {
				logger.error("error in test helper");
				DTThrowable.rethrow(e);
			}

		}
	}

	public static void clearTable() {
		TableOperations tableoper = con.tableOperations();
		if (!tableoper.exists("tab1")) {
			try {
				tableoper.create("tab1");
			} catch (Exception e) {
				logger.error("error in test helper");
				DTThrowable.rethrow(e);
			}
		}
		try {
			tableoper.deleteRows("tab1", null, null);
		} catch (AccumuloException e) {
			logger.error("error in test helper");
			DTThrowable.rethrow(e);
		} catch (AccumuloSecurityException e) {
			logger.error("error in test helper");
			DTThrowable.rethrow(e);
		} catch (TableNotFoundException e) {
			logger.error("error in test helper");
			DTThrowable.rethrow(e);
		}
	}

	public static void populateAccumulo() throws IOException {
		BatchWriterConfig config = new BatchWriterConfig();
		BatchWriter batchwriter = null;
		try {
			batchwriter = con.createBatchWriter("tab1", config);
		} catch (TableNotFoundException e) {
			logger.error("error in test helper");
			DTThrowable.rethrow(e);
		}
		try {
			for (int i = 0; i < 500; ++i) {
				Mutation mutation = new Mutation(Bytes.toBytes("row" + i));
				for (int j = 0; j < 500; ++j) {
					mutation.put(colfam0_bytes, Bytes.toBytes("col" + "-" + j),
							System.currentTimeMillis(),
							Bytes.toBytes("val" + "-" + i + "-" + j));
				}
				batchwriter.addMutation(mutation);
			}
			batchwriter.close();
		} catch (MutationsRejectedException e) {
			logger.error("error in test helper");
			DTThrowable.rethrow(e);
		}
	}

	public static AccumuloTuple findTuple(List<AccumuloTuple> tuples,
			String row, String colFamily, String colName) {
		AccumuloTuple mtuple = null;
		for (AccumuloTuple tuple : tuples) {
			if (tuple.getRow().equals(row)
					&& tuple.getColFamily().equals(colFamily)
					&& tuple.getColName().equals(colName)) {
				mtuple = tuple;
				break;
			}
		}
		return mtuple;
	}

	public static void deleteTable() {
		TableOperations tableoper = con.tableOperations();
		if (tableoper.exists("tab1")) {

			try {
				tableoper.delete("tab1");
			} catch (AccumuloException e) {
				logger.error("error in test helper");
				DTThrowable.rethrow(e);
			} catch (AccumuloSecurityException e) {
				logger.error("error in test helper");
				DTThrowable.rethrow(e);
			} catch (TableNotFoundException e) {
				logger.error("error in test helper");
				DTThrowable.rethrow(e);
			}

		}
	}

	public static void getConnector() {
		Instance instance = new ZooKeeperInstance("milind", "127.0.0.1");
		try {
			con = instance.getConnector("root", "milind");
		} catch (AccumuloException e) {
			logger.error("error in test helper");
			DTThrowable.rethrow(e);
		} catch (AccumuloSecurityException e) {
			logger.error("error in test helper");
			DTThrowable.rethrow(e);
		}

	}

	public static AccumuloTuple getAccumuloTuple(String row, String colFam,
			String colName) {
		Authorizations auths = new Authorizations();

		Scanner scan = null;
		try {
			scan = con.createScanner("tab1", auths);
		} catch (TableNotFoundException e) {
			logger.error("error in test helper");
			DTThrowable.rethrow(e);
		}

		scan.setRange(new Range(new Text(row)));
		scan.fetchColumn(new Text(colFam), new Text(colName));
		// assuming only one row
		for (Entry<Key, Value> entry : scan) {
			AccumuloTuple tuple = new AccumuloTuple();
			tuple.setRow(entry.getKey().getRow().toString());
			tuple.setColFamily(entry.getKey().getColumnFamily().toString());
			tuple.setColName(entry.getKey().getColumnQualifier().toString());
			tuple.setColValue(entry.getValue().toString());
			return tuple;
		}
		return null;
	}
}