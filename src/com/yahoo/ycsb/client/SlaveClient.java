package com.yahoo.ycsb.client;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;

import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.measurements.OneMeasurement;
import com.yahoo.ycsb.rmi.PropertyPackage;
import com.yahoo.ycsb.rmi.RMIInterface;

public class SlaveClient implements RMIInterface, ClientStatus {
	public static final String REGISTRY_NAME = "LoadGeneratorRMIInterface"; 
	private static SlaveClient client = null;
	
	private int status;
	private LoadThread thread;
	private PropertyPackage proppkg;

	private SlaveClient() {
		status = NOT_STARTED;
		thread = null;
		
		try {
            RMIInterface stub = (RMIInterface) UnicastRemoteObject.exportObject(this, 0);
            Registry registry = LocateRegistry.getRegistry();
            registry.rebind(REGISTRY_NAME, stub);
        } catch (Exception e) {
            System.err.println("ComputeEngine exception:");
            e.printStackTrace();
        }
	}
	
	public static SlaveClient getSlaveClient() {
		if (client == null) {
			client = new SlaveClient();
			return client;
		}
		return client;
	}
	
	@Override
	public HashMap<String, OneMeasurement> getCurrentStats() {
		if (thread != null && thread.getState() != Thread.State.TERMINATED)
			return Measurements.getMeasurements().getAndResetPartialData();
		else
			return null;
	}
	
	@Override
	public int setProperties(PropertyPackage proppkg) {
		if (proppkg == null)
			return -1;
		client.proppkg = proppkg;
		client.status = READY;
		return 0;
	}
	
	@Override
	public int execute() {
		if (client.proppkg == null)
			return -2;
		if (client.thread == null || client.thread.getState() == Thread.State.TERMINATED)
			client.thread = new LoadThread(client.proppkg);
		else
			return -1;
		client.status = RUNNING;
		client.thread.start();
		return 0;
	}

	@Override
	public int getStatus() {
		return client.status;
	}
	
	public void setStatus(int status) {
		client.status = status;
	}
	
	public static void main(String args[]) {
		SlaveClient client = getSlaveClient();
	}
}

