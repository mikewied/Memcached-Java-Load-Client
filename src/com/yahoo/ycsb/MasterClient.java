package com.yahoo.ycsb;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.LinkedList;
import java.util.List;

import com.yahoo.ycsb.rmi.PropertyPackage;
import com.yahoo.ycsb.rmi.RMIInterface;

public class MasterClient extends Client{
	LoadThread thread;
	List<Registry> registry;
	String[] address;
	
	
	public MasterClient(PropertyPackage proppkg) {
		super(proppkg);
		String addresses  = proppkg.props.getProperty("slaveaddress", null);
		
		if (addresses != null)
			address = addresses.split(",");
		else
			address = null;
		registry = new LinkedList<Registry>();
	}
	
	public void init() {
		if (address != null) {
			for (int i = 0; i < address.length; i++) {
				try {
		            System.out.println("Setting up Master");
		            System.out.println(address[i]);
		            registry.add(LocateRegistry.getRegistry(address[i]));
		            
		        } catch (Exception e) {
		            System.err.println("Could not connect to slave on " + address[i]);
		        }
			}
	        setupSlaves();
		}
		execute();
	}
	
	private void setupSlaves() {
		int res;
		for (int i = 0; i < registry.size(); i++) {
			try {
				RMIInterface loadgen;
				loadgen = (RMIInterface) registry.get(i).lookup(REGISTRY_NAME);
				res = loadgen.setProperties(proppkg);
				if (res == 0)
					System.out.println("Slave Got Property Package");
				else if (res == -1)
					System.out.println("Property Package sent to Slave was NULL");
				else
					System.out.println("Unknown Error Code");
			} catch (NotBoundException e) {
				System.out.println("Setup Slaves: Slave is not running");
			}catch (RemoteException e) {
				System.out.println("Setup Slaves: Cannot connect to slave");
			}
		}
	}

	@Override
	public int setProperties(PropertyPackage proppkg) {

		return 0;
	}
	
	@Override
	public int execute() {
		int res;
		for (int i = 0; i < registry.size(); i++) {
			try {
				RMIInterface loadgen;
				loadgen = (RMIInterface) registry.get(i).lookup(REGISTRY_NAME);
				res = loadgen.execute();
				if (res == 0)
					System.out.println("Load Generation Started On Slave");
				else if (res == -1)
					System.out.println("No available threads: Max is 5");
				else if (res == -2)
					System.out.println("Slave has no props file");
				else
					System.out.println("Unknown Error Code");
			} catch (NotBoundException e) {
				System.out.println("Execute: Slave is not running");
			}catch (RemoteException e) {
				System.out.println("Execute: Cannot connect to slave");
			}
		}
		return 0;
	}
}
