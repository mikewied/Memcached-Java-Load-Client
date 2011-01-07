package com.yahoo.ycsb.rmi;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.yahoo.ycsb.client.Client;

public class LoadProperties implements Serializable {
	private static final long serialVersionUID = 5800547687640426688L;
	
	public static final String DB_NAME = "db";
	public static final String DO_TRANSACTIONS = "dotransactions";
	public static final String INSERT_COUNT = "insertcount";
	public static final String LABEL = "label";
	public static final String OP_COUNT = "operationcount";
	public static final String RECORD_COUNT = "recordcount";
	public static final String TARGET = "target";
	public static final String THREAD_COUNT = "threadcount";
	public static final String WORKLOAD = "workload";
	public static final int MASTER_CONFIG = 0;
	
	private int clients;
	private Properties props;
	private List<Properties> client_config;
	
	public LoadProperties(Properties props, int clients) {
		this.props = props;
		this.clients = clients;
		client_config = new LinkedList<Properties>();
		
		for (int i = 0; i < clients; i++)
			client_config.add(new Properties());
		
		Set<String> pset = props.stringPropertyNames();
		Iterator<String> itr = pset.iterator();
		
		while (itr.hasNext()) {
			String key = itr.next();
			String value = (String)props.get(key);
			if (clients > 1 && (key.equals(THREAD_COUNT) || key.equals(OP_COUNT) || key.equals(TARGET) )) {
				int val =  Integer.parseInt(value) / clients;
				int remainder = Integer.parseInt(value) % clients;
				
				for (int i = 0; i < clients; i++) {
					if (remainder > 0) {
						client_config.get(i).put(key, String.valueOf(val + 1));
						remainder--;
					} else {
						client_config.get(i).put(key, String.valueOf(val));
					}
				}
			} else {
				for (int i = 0; i < clients; i++)
					client_config.get(i).put(key, String.valueOf(value));
			}	
		}
	}
	
	public void printConfigs() {
		for (int i = 0; i < client_config.size(); i++) {
			System.out.println("\nClient " + i + "\n\n");
			Set<String> pset = client_config.get(i).stringPropertyNames();
			Iterator<String> itr = pset.iterator();
			while (itr.hasNext()) {
				String key = itr.next();
				String value = (String)client_config.get(i).get(key);
				System.out.println("Key: " + key + " Value: " + value);
			}
		}
	}
	
	public Properties getConfig(int index) {
		return client_config.get(index);
	}
	
	/*private void init(Properties props) {
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
		
	}*/
	
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
}
