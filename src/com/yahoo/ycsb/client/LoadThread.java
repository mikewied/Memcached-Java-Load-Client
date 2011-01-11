/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package com.yahoo.ycsb.client;

import java.util.*;

import com.yahoo.ycsb.DataStore;
import com.yahoo.ycsb.UnknownDataStoreException;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.database.DBFactory;
import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.memcached.MemcachedFactory;
//import org.apache.log4j.BasicConfigurator;
import com.yahoo.ycsb.rmi.LoadProperties;

/**
 * Main class for executing YCSB.
 */
public class LoadThread extends Thread {

	static long printstatsinterval;
	Workload workload;
	public Properties props;
	public Vector<Thread> threads;
	
	public LoadThread(Properties props) {
		this.props = props;
		workload = null;
		threads = new Vector<Thread>();
		init();
	}
		

	public void init() {
		String workloadloc = props.getProperty(LoadProperties.WORKLOAD);
		String dbname = props.getProperty(LoadProperties.DB_NAME);
		boolean dotransactions = Boolean.parseBoolean(props.getProperty(LoadProperties.DO_TRANSACTIONS));
		int opcount = Integer.parseInt(props.getProperty(LoadProperties.OP_COUNT));
		int threadcount = Integer.parseInt((String)props.getProperty(LoadProperties.THREAD_COUNT));
		int target = Integer.parseInt(props.getProperty(LoadProperties.TARGET));
		int recordcount = Integer.parseInt(props.getProperty(LoadProperties.RECORD_COUNT));
		String protocol = props.getProperty(Client.PROTOCOL_PROPERTY);

		// compute the target throughput
		double targetperthreadperms = -1;
		if (target > 0) {
			double targetperthread = ((double) target) / ((double) threadcount);
			targetperthreadperms = targetperthread / 1000.0;
		}

		Measurements.setProperties(props);

		ClassLoader classLoader = Client.class.getClassLoader();
		try {
			@SuppressWarnings("rawtypes")
			Class workloadclass = classLoader.loadClass(workloadloc);
			workload = (Workload) workloadclass.newInstance();
		} catch (Exception e) {
			e.printStackTrace();
			e.printStackTrace(System.out);
			System.exit(0);
		}
		try {
			workload.init(props);
		} catch (WorkloadException e) {
			e.printStackTrace();
			e.printStackTrace(System.out);
			System.exit(0);
		}

		
		for (int threadid = 0; threadid < threadcount; threadid++) {
			DataStore db = null;
			try {
				if (workloadloc.equals("com.yahoo.ycsb.workloads.MemcachedCoreWorkload"))
					db = MemcachedFactory.newMemcached(dbname, props);
				else if (protocol.equals("db"))
					db = DBFactory.newDB(dbname, props);
				else {
					System.out.println("Invalid Protocol: " + protocol);
					System.exit(0);
				}
			} catch (UnknownDataStoreException e) {
				System.out.println("Unknown DB " + dbname);
				System.exit(0);
			}
			Thread t;
			if (dotransactions)
				t = new ClientThread(db, dotransactions, workload, threadid, threadcount, props, opcount / threadcount, targetperthreadperms);
			else
				t = new ClientThread(db, dotransactions, workload, threadid, threadcount, props, recordcount / threadcount, targetperthreadperms);
			threads.add(t);
		}
	}
	
	public void run() {
		// Start all of the worker threads
		for (Thread t : threads) {
			t.start();
		}
		// Wait for all of the worker threads to complete
		for (Thread t : threads) {
			try {
				t.join();
			} catch (InterruptedException e) {}
		}
		// Wait until the status thread grabs the last piece of stats data
		while (Measurements.getMeasurements().getPartialData().size() > 0) {
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		// Cleanup the worker threads workspace
		try {
			workload.cleanup();
		} catch (WorkloadException e) {
			e.printStackTrace(System.out);
			System.exit(0);
		}
	}
}
