package com.datatorrent.contrib.accumulo;


import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.datatorrent.api.annotation.ShipContainingJars;
import com.datatorrent.lib.db.AbstractStoreInputOperator;

@ShipContainingJars(classes = {org.apache.accumulo.core.client.Connector.class})
public abstract class AbstractAccumuloInputOperator<T> extends AbstractStoreInputOperator<T, AccumuloStore> {
	
	public abstract T getTuple(Entry<Key,Value> entry);
	
	public abstract Scanner getScanner(Connector conn);
	
	public AbstractAccumuloInputOperator(){
		store=new AccumuloStore();
	}
	@Override
	public void emitTuples() {
		Connector conn=getStore().getConnector();
		Scanner scan=getScanner(conn);

		for(Entry<Key,Value> entry : scan){
			T tuple = getTuple(entry);
			outputPort.emit(tuple);
		}
		    
		
	}

}
