package com.yahoo.ycsb;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Properties;

import com.yahoo.ycsb.rmi.RMIInterface;

public class MasterClient extends Client{
	LoadThread thread;
	Registry registry;
	
	public MasterClient(Properties props, boolean dotransactions, int threadcount, int target, boolean status, boolean slave, String label) {
		super(props, dotransactions, threadcount, target, status, label);
		thread = new LoadThread(props, dotransactions, threadcount, target, status, label);
	}
	
	public void init() {
		//if (System.getSecurityManager() == null) {
        //    System.setSecurityManager(new SecurityManager());
        //}
		String slaveAddresses = props.getProperty("slaveaddress", null);
		
		if (slaveAddresses != null) {
			try {
	            System.out.println("Setting up Master");
	            registry = LocateRegistry.getRegistry(slaveAddresses);
	        } catch (Exception e) {
	            System.err.println("Could not connect to slave on " + slaveAddresses);
	        }
	        setupSlaves();
		}
		execute();
	}
	
	private void setupSlaves() {
		
	}
	
	public String execute() {
		String res;
		try {
			RMIInterface loadgen;
			loadgen = (RMIInterface) registry.lookup(REGISTRY_NAME);
			res = loadgen.execute();
			System.out.println(res);
		} catch (NotBoundException e) {
			e.printStackTrace();
		}catch (RemoteException e) {
			e.printStackTrace();
		}
		return "Starting Load Generation on Master Client";
	}
}
