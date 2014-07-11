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

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.db.Connectable;

/**
 * A {@link Connectable} for accumulo
 *
 */
public class AccumuloStore implements Connectable {
	private static final transient Logger logger = LoggerFactory
			.getLogger(AccumuloStore.class);
	private String zookeeperHost;
	private String instanceName;
	private String userName;
	private String password;
	protected String tableName;
	protected transient Connector connector;

	/**
	 * getter for Connector
	 * 
	 * @return Connector
	 */
	public Connector getConnector() {
		return connector;
	}

	/**
	 * getter for TableName
	 * 
	 * @return TableName
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * setter for TableName
	 * 
	 * @param tableName
	 */
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	/**
	 * getter for zookeeper host address
	 * 
	 * @return ZookeeperHost
	 */
	public String getZookeeperHost() {
		return zookeeperHost;
	}

	/**
	 * setter for zookeeper host address
	 * 
	 * @param zookeeperHost
	 */
	public void setZookeeperHost(String zookeeperHost) {
		this.zookeeperHost = zookeeperHost;
	}

	/**
	 * getter for instanceName
	 * 
	 * @return instanceName
	 */
	public String getInstanceName() {
		return instanceName;
	}

	/**
	 * setter for instanceName
	 * 
	 * @param instanceName
	 */
	public void setInstanceName(String instanceName) {
		this.instanceName = instanceName;
	}

	/**
	 * setter for userName
	 * 
	 * @param userName
	 */
	public void setUserName(String userName) {
		this.userName = userName;
	}

	/**
	 * setter for password
	 * 
	 * @param password
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	@Override
	public void connect() throws IOException {
		Instance instance = null;
		instance = new ZooKeeperInstance(instanceName, zookeeperHost);
		try {
			PasswordToken t = new PasswordToken(password.getBytes());
			connector = instance.getConnector(userName, t);
		} catch (AccumuloException e) {
			logger.error("error connecting to accumulo", e);
			DTThrowable.rethrow(e);
		} catch (AccumuloSecurityException e) {
			logger.error("error connecting to accumulo", e);
			DTThrowable.rethrow(e);
		}

	}

	@Override
	public void disconnect() throws IOException {
		// No disconnect for accumulo
	}

	@Override
	public boolean connected() {
		// Not applicable for accumulo
		return false;
	}

}
