package com.yahoo.ycsb.client;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import com.yahoo.ycsb.client.Client;
import com.yahoo.ycsb.client.LoadThread;
import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.measurements.OneMeasurement;
import com.yahoo.ycsb.rmi.PropertyPackage;
import com.yahoo.ycsb.rmi.RMIInterface;

public class MasterClient extends Client implements ClientStatus{
	LoadThread thread;
	String[] address;
	
	// Maps IP addresses to registry instances
	HashMap<String, Registry> rmiClients;
	
	
	public MasterClient(PropertyPackage proppkg) {
		super(proppkg);
		System.out.println("Client Type: Master");
		rmiClients = new HashMap<String, Registry>();
		
		String addresses  = proppkg.props.getProperty("slaveaddress", null);
		if (addresses != null)
			address = addresses.split(",");
		else
			address = null;
		
		if (address != null) {
			for (int i = 0; i < address.length; i++) {
				try {
		            Registry registry = LocateRegistry.getRegistry(address[i]);
		            if (registry != null)
		            	rmiClients.put(address[i], registry);
		            
		        } catch (RemoteException e) {
		            System.err.println("Could not connect to slave client at " + address[i]);
		        }
			}
	        setupSlaves();
		}
	}
	
	private void setupSlaves() {
		int res;
		boolean dotransactions = Boolean.parseBoolean(proppkg.getProperty(PropertyPackage.DO_TRANSACTIONS));
		int threadcount = Integer.parseInt(proppkg.getProperty(PropertyPackage.THREAD_COUNT));
		int target = Integer.parseInt(proppkg.getProperty(PropertyPackage.TARGET));
		PropertyPackage pkg = new PropertyPackage(proppkg.props, dotransactions, threadcount, target, true, proppkg.label);
		
		Set<String> keys = rmiClients.keySet();
		Iterator<String> itr = keys.iterator();
		
		while (itr.hasNext()) {
			String key = itr.next();
			try {
				RMIInterface loadgen = (RMIInterface) rmiClients.get(key).lookup(SlaveClient.REGISTRY_NAME);
				res = loadgen.setProperties(pkg);
				if (res != 0)
					System.out.println("Property Package sent to Slave was NULL");
				else if (res != 0)
					System.out.println("Setup: Unknown Error Code");
			} catch (NotBoundException e) {
				System.out.println("Could not send properties to " + key + " because slave was not bound");
			}catch (RemoteException e) {
				System.out.println("Could not send properties to " + key + " because slave is not running");
			}
		}
	}
	
	@Override
	public void execute() {
		int res;
		
		Set<String> keys = rmiClients.keySet();
		Iterator<String> itr = keys.iterator();
		
		while (itr.hasNext()) {
			String key = itr.next();
			try {
				RMIInterface loadgen = (RMIInterface) rmiClients.get(key).lookup(SlaveClient.REGISTRY_NAME);
				res = loadgen.execute();
				if (res != 0)
					System.out.println("Error executing slave");
			} catch (NotBoundException e) {
				System.out.println("Could not run test with " + key + " because slave was not bound");
			}catch (RemoteException e) {
				System.out.println("Could not run test with " + key + " because slave is not running");
			}
		}
		thread = new LoadThread(proppkg);
		thread.start();
		StatusThread status = new StatusThread(thread, rmiClients);
		status.start();
		
		try {
			thread.join();
			status.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public HashMap<String, OneMeasurement> getCurrentStats() {
		return null;
	}
	
	@Override
	public int setProperties(PropertyPackage proppkg) {
		return 0;
	}

	@Override
	public int getStatus() {
		// TODO Auto-generated method stub
		return 0;
	}
}
