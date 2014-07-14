package com.datatorrent.contrib.accumulo;

import java.util.ArrayList;
import java.util.List;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;

public class AccumuloTupleCollector extends BaseOperator {

	public static List<AccumuloTuple> tuples;

	public AccumuloTupleCollector() {
		tuples = new ArrayList<AccumuloTuple>();
	}

	public final transient DefaultInputPort<AccumuloTuple> inputPort = new DefaultInputPort<AccumuloTuple>() {
		public void process(AccumuloTuple tuple) {
			tuples.add(tuple);
		}
	};

}