package com.yahoo.ycsb.client;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
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
	private LoadThread lt;
	private PropertyPackage proppkg;
	private Registry registry;

	private SlaveClient() {
		status = NOT_STARTED;
		lt = null;
		
		try {
            RMIInterface stub = (RMIInterface) UnicastRemoteObject.exportObject(this, 0);
            LocateRegistry.createRegistry(1099);
            registry = LocateRegistry.getRegistry();
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
		if (lt != null && lt.getState() != Thread.State.TERMINATED) {
			System.out.println("Ops Done: " + Measurements.getMeasurements().getOperations());
			return Measurements.getMeasurements().getAndResetPartialData();
		} else {
			return null;
		}
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
		if (lt == null || lt.getState() == Thread.State.TERMINATED) {
			lt = new LoadThread(proppkg, true);
		} else {
			return -1;
		}
		status = RUNNING;
		lt.start();
		return 0;
	}

	@Override
	public int getStatus() {
		return client.status;
	}
	
	public void setStatus(int status) {
		client.status = status;
	}
	
	public void shutdown() {
		try {
			registry.unbind(REGISTRY_NAME);
			UnicastRemoteObject.unexportObject(this, true);
		} catch (AccessException e) {
			e.printStackTrace();
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("unused")
	public static void main(String args[]) {
		try {
		    InetAddress addr = InetAddress.getLocalHost();
		    System.out.println("Binding to: " + addr.getHostAddress());
		    System.setProperty("java.rmi.server.hostname", addr.getHostAddress());
		} catch (UnknownHostException e) {
			System.out.println("I can't get my IP address");
		}
		
		SlaveClient client = getSlaveClient();
	}
}

