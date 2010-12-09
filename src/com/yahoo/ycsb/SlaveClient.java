package com.yahoo.ycsb;

import java.rmi.AccessException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Properties;

import com.yahoo.ycsb.rmi.PropertyPackage;
import com.yahoo.ycsb.rmi.RMIImpl;
import com.yahoo.ycsb.rmi.RMIInterface;

public class SlaveClient extends Client {
	
	LoadThread[] threads;
	
	RMIInterface engine;
	RMIInterface stub;
	Registry registry;
	
	public SlaveClient() {
		super(null);
		threads = new LoadThread[MAX_LOAD_THREADS];
	}
	
	public void init() {
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
	
	@Override
	public int setProperties(PropertyPackage proppkg) {
		if (proppkg == null)
			return -1;
		this.proppkg = proppkg;
		return 0;
	}
	
	@Override
	public int execute() {
		if (proppkg == null)
			return -2;
		LoadThread thread = getAvailableThread();
		System.out.println(thread);
		if (thread == null)
			return -1;
		thread.start();
		return 0;
	}
	
	private LoadThread getAvailableThread() {
		for (int i = 0; i < MAX_LOAD_THREADS; i++) {
			if (threads[i] == null || threads[i].getState() == Thread.State.TERMINATED) {
				threads[i] = new LoadThread(proppkg);
				return threads[i];
			}
		}
		return null;
	}

	
}
