package com.yahoo.ycsb.client;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.measurements.OneMeasurement;
import com.yahoo.ycsb.rmi.RMIInterface;

/**
 * A thread to periodically show the status of the experiment, to reassure you
 * that progress is being made.
 * 
 * @author cooperb
 * 
 */
public class StatusThread extends Thread {
	HashMap<String, Registry> rmiClients;
	Vector<Thread> _threads;
	String _label;
	boolean _standardstatus;
	long _printstatsinterval;

	/**
	 * The interval for reporting status.
	 */
	

	public StatusThread(LoadThread lt, HashMap<String, Registry> rmiClients) {
		this.rmiClients = rmiClients;
		_threads = lt.threads;
		_label = lt.proppkg.label;
		_printstatsinterval = 5;
	}

	/**
	 * Run and periodically report status.
	 */
	public void run() {
		long st = System.currentTimeMillis();
		boolean alldone;

		do {
			alldone = true;
			
			Set<String> keys = rmiClients.keySet();
			Iterator<String> itr = keys.iterator();
			
			while (itr.hasNext()) {
				HashMap<String, OneMeasurement> res = null;
				String key = itr.next();
				try {
					RMIInterface loadgen = (RMIInterface) rmiClients.get(key).lookup(SlaveClient.REGISTRY_NAME);
					res = loadgen.getCurrentStats();
					if (res != null) {
						Measurements.getMeasurements().add(res);
						alldone = false;
					}
				} catch (NotBoundException e) {
					System.out.println("Could not get stats from " + key + " because slave was not bound");
				}catch (RemoteException e) {
					System.out.println("Could not get stats from " + key + " because slave is not running");
				}

			}

			// terminate this thread when all the worker threads are done
			for (Thread t : _threads) {
				if (t.getState() != Thread.State.TERMINATED) {
					alldone = false;
				}
			}

			long en = System.currentTimeMillis();
			long interval = en - st;
			System.err.println(_label + " " + (interval / 1000) + " sec: " + Measurements.getMeasurements().getSummary() + "\n");

			try {
				sleep(_printstatsinterval * 1000);
			} catch (InterruptedException e) {}

		} while (!alldone);
	}
}