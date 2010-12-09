package com.yahoo.ycsb.rmi;

import java.io.Serializable;
import java.util.Properties;

public class PropertyPackage implements Serializable {
	private static final long serialVersionUID = 5800547687640426688L;
	
	public String dbname;
	public Properties props;
	public boolean dotransactions;
	public int threadcount;
	public int target;
	public boolean status;
	public String label;
	
	public PropertyPackage(Properties props, boolean dotransactions, int threadcount, int target, boolean status, boolean slave, String label) {
		this.props = props;
		this.dotransactions = dotransactions;
		this.threadcount = threadcount;
		this.target = target;
		this.status = status;
		this.label = label;
	}
}
