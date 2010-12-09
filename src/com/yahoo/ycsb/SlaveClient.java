package com.yahoo.ycsb;

import java.rmi.AccessException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Properties;

import com.yahoo.ycsb.rmi.RMIImpl;
import com.yahoo.ycsb.rmi.RMIInterface;

public class SlaveClient extends Client {
	
	LoadThread[] threads;
	
	RMIInterface engine;
	RMIInterface stub;
	Registry registry;
	
	public SlaveClient(Properties props, boolean dotransactions, int threadcount, int target, boolean status, boolean slave, String label) {
		super(props, dotransactions, threadcount, target, status, label);
		threads = new LoadThread[MAX_LOAD_THREADS];
	}
	
	public void init() {
		//if (System.getSecurityManager() == null) {
        //    System.setSecurityManager(new SecurityManager());
        //}
		
		try {
            engine = new RMIImpl(this);
            stub = (RMIInterface) UnicastRemoteObject.exportObject(engine, 0);
            System.out.println("Locating Registry");
            registry = LocateRegistry.getRegistry();
            System.out.println("Rebinding Registry");
            registry.rebind(REGISTRY_NAME, stub);
            System.out.println("Registry Length: " + registry.list().length);
            for (int i = 0; i < registry.list().length; i++) System.out.println(registry.list()[i]);
            System.out.println("ComputeEngine bound");
        } catch (Exception e) {
            System.err.println("ComputeEngine exception:");
            e.printStackTrace();
        }
	}
	
	public void setProps() {
		
	}
	
	public String execute() {
		LoadThread thread = getAvailableThread();
		System.out.println(thread);
		if (thread == null)
			return "No available threads: Max is 5";
		thread.start();
		return "Load Generation Started On Slave";
	}
	
	private LoadThread getAvailableThread() {
		for (int i = 0; i < MAX_LOAD_THREADS; i++) {
			if (threads[i] == null || threads[i].getState() == Thread.State.TERMINATED) {
				threads[i] = new LoadThread(props, dotransactions, threadcount, target, status, label);
				return threads[i];
			}
		}
		return null;
	}
}
