package com.yahoo.ycsb.client;

import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import com.yahoo.ycsb.client.LoadThread;
import com.yahoo.ycsb.rmi.LoadProperties;
import com.yahoo.ycsb.rmi.MasterRMIInterface;
import com.yahoo.ycsb.rmi.SlaveRMIInterface;

public class MasterClient implements MasterRMIInterface {
	public static final String REGISTRY_NAME = "MasterRMIInterface";
	public static MasterClient client = null;
	private static final int RMI_PORT = 1098;
	private Registry registry;
	
	LoadProperties props;
	LoadThread lt;
	StatusThread st;
	HashMap<String, Registry> rmiClients;
	
	private MasterClient() {
		//super(null);
		lt = null;
		st = null;
		rmiClients = new HashMap<String, Registry>();
	}
	
	public static MasterClient getMasterClient() {
		if (client == null)
			client = new MasterClient();
		return client;
	}
	
	public void init(Properties p) {
		initRMI();
		initSlaveRMI(p.getProperty("slaveaddress", null));
		this.props = new LoadProperties(p, rmiClients.size() + 1);
	}
	
	private void initRMI() {
		try {
            MasterRMIInterface stub = (MasterRMIInterface) UnicastRemoteObject.exportObject(this, 0);
            LocateRegistry.createRegistry(RMI_PORT);
            registry = LocateRegistry.getRegistry();
            registry.rebind(REGISTRY_NAME, stub);
        } catch (Exception e) {
            System.err.println("SlaveRMI exception:");
            e.printStackTrace();
        }
	}
	
	private void initSlaveRMI(String addresses) {
		String[] address;
		
		if (addresses != null) {
			address = addresses.split(",");
			for (int i = 0; i < address.length; i++) {
				try {
		            Registry registry = LocateRegistry.getRegistry(address[i]);
		            registry.lookup(SlaveClient.REGISTRY_NAME);
		            if (registry != null)
		            	rmiClients.put(address[i], registry);
		        } catch (RemoteException e) {
		            System.err.println("Could not connect to slave client at " + address[i]);
		        } catch (NotBoundException e) {
		        	System.err.println("Slave Client not bound at " + address[i]);
				}
			}
		}
	}
	
	public void setupSlaves() {
		int res;
		Set<String> keys = rmiClients.keySet();
		Iterator<String> itr = keys.iterator();
		
		for (int i = 1; itr.hasNext(); i++) {
			String key = itr.next();
			try {
				SlaveRMIInterface loadgen = (SlaveRMIInterface) rmiClients.get(key).lookup(SlaveClient.REGISTRY_NAME);
				res = loadgen.setProperties(props.getConfig(i));
				if (res != 0)
					System.out.println("Properties sent to Slave were NULL");
			} catch (NotBoundException e) {
				System.out.println("Could not send properties to " + key + " because slave was not bound\nRemoving slave node from setup");
				rmiClients.remove(key);
			}catch (RemoteException e) {
				System.out.println("Could not send properties to " + key + " because slave is not running\nRemoving slave node from setup");
				rmiClients.remove(key);
			}
		}
	}
	
	public void execute() {
		int res;
		Set<String> keys = rmiClients.keySet();
		Iterator<String> itr = keys.iterator();
		
		while (itr.hasNext()) {
			String key = itr.next();
			try {
				SlaveRMIInterface loadgen = (SlaveRMIInterface) rmiClients.get(key).lookup(SlaveClient.REGISTRY_NAME);
				res = loadgen.execute();
				if (res != 0)
					System.out.println("Error executing slave");
			} catch (NotBoundException e) {
				System.out.println("Could not run test with " + key + " because slave was not bound");
			}catch (RemoteException e) {
				System.out.println("Could not run test with " + key + " because slave is not running");
			}
		}
		lt = new LoadThread(props.getConfig(LoadProperties.MASTER_CONFIG));
		st = new StatusThread(lt, rmiClients, props.getConfig(LoadProperties.MASTER_CONFIG));
		st.start();
		
		try {
			st.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void shutdownSlaves() {
		Set<String> keys = rmiClients.keySet();
		Iterator<String> itr = keys.iterator();
		
		while (itr.hasNext()) {
			String key = itr.next();
			try {
				SlaveRMIInterface loadgen = (SlaveRMIInterface) rmiClients.get(key).lookup(SlaveClient.REGISTRY_NAME);
				loadgen.shutdown();
			} catch (NotBoundException e) {
				System.out.println("Could not run test with " + key + " because slave was not bound");
			}catch (RemoteException e) {
				System.out.println("Could not run test with " + key + " because slave is not running");
			}
		}
	}
	
	public void shutdownMaster() {
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

	@Override
	public void changeWorkingSet(int workingset) throws RemoteException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void changeThroughput(int throughput) throws RemoteException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void changeItemCount(int delta, int seconds, int maxitems)
			throws RemoteException {
		// TODO Auto-generated method stub
		
	}
}
