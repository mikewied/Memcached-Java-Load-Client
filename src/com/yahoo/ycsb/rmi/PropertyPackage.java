package com.yahoo.ycsb.rmi;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Properties;

import com.yahoo.ycsb.client.Client;

public class PropertyPackage implements Serializable {
	private static final long serialVersionUID = 5800547687640426688L;
	
	public static final String DB_NAME = "db";
	public static final String DO_TRANSACTIONS = "dotransactions";
	public static final String INSERT_COUNT = "insertcount";
	public static final String OP_COUNT = "operationcount";
	public static final String RECORD_COUNT = "recordcount";
	public static final String TARGET = "target";
	public static final String THREAD_COUNT = "threadcount";
	public static final String WORKLOAD = "workload";
	
	private HashMap<String, String> properties;
	public String dbname;
	public Properties props;
	public boolean slave;
	public String label;
	
	public PropertyPackage(Properties props, boolean dotransactions, int threadcount, int target, boolean slave, String label) {
		this.props = props;
		this.properties = new HashMap<String, String>();
		properties.put(DO_TRANSACTIONS, Boolean.toString(dotransactions));
		properties.put(THREAD_COUNT, Integer.toString(threadcount));
		properties.put(TARGET, Integer.toString(target));
		this.slave = slave;
		this.label = label;
		init(props);
	}
	
	private void init(Properties props) {
		properties.put(DB_NAME, props.getProperty(DB_NAME, "com.yahoo.ycsb.BasicDB"));
		properties.put(INSERT_COUNT, props.getProperty(INSERT_COUNT, "0"));
		properties.put(RECORD_COUNT, props.getProperty(RECORD_COUNT, "0"));
		properties.put(THREAD_COUNT, props.getProperty(THREAD_COUNT, "1"));
		properties.put(TARGET, props.getProperty(TARGET, "0"));
		if (!checkRequiredProperties(props)) {
			System.exit(0);
		}
		properties.put(WORKLOAD, props.getProperty(WORKLOAD));
		
		if (Boolean.parseBoolean(getProperty(DO_TRANSACTIONS))) {
			properties.put(OP_COUNT, props.getProperty(OP_COUNT, "0"));
		} else {
			if (props.containsKey(INSERT_COUNT)) {
				properties.put(OP_COUNT, props.getProperty(INSERT_COUNT, "0"));
			} else {
				properties.put(OP_COUNT, props.getProperty(RECORD_COUNT, "0"));
			}
		}
		
	}
	
	public static boolean checkRequiredProperties(Properties props) {
		if (props.getProperty(WORKLOAD) == null) {
			System.out.println("Missing property: " + Client.WORKLOAD_PROPERTY);
			return false;
		}
		if (props.getProperty(Client.PROTOCOL_PROPERTY) == null) {
			System.out.println("Missing property: " + Client.PROTOCOL_PROPERTY);
			return false;
		}
		return true;
	}
	
	public String getProperty(String key) {
		return properties.get(key);
	}
}
