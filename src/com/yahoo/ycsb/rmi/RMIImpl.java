package com.yahoo.ycsb.rmi;

import java.rmi.RemoteException;

import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.SlaveClient;

public class RMIImpl implements RMIInterface{
	Client client;
	
	public RMIImpl(SlaveClient client) {
		super();
		this.client = client;
	}
	
	@Override
	public int execute() {
		return client.execute();
	}
	
	public int setProperties(PropertyPackage proppkg) {
		return client.setProperties(proppkg);
	}

}
