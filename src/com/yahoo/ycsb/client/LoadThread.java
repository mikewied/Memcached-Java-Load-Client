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

import java.io.*;
import java.util.*;

import com.yahoo.ycsb.DataStore;
import com.yahoo.ycsb.DataStoreException;
import com.yahoo.ycsb.UnknownDataStoreException;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.database.DBFactory;
import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.measurements.OneMeasurement;
import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;
import com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter;
import com.yahoo.ycsb.memcached.MemcachedFactory;
//import org.apache.log4j.BasicConfigurator;
import com.yahoo.ycsb.rmi.PropertyPackage;

/**
 * Main class for executing YCSB.
 */
public class LoadThread extends Thread {

	static long printstatsinterval;
	Workload workload;
	public PropertyPackage proppkg;
	private Thread statusthread;
	public Vector<Thread> threads;
	DataStore db;
	
	public LoadThread(PropertyPackage proppkg) {
		this.proppkg = proppkg;
		workload = null;
		statusthread = null;
		threads = new Vector<Thread>();
		db = null;
		init();
	}
		

	public void init() {
		String workloadloc = proppkg.getProperty(PropertyPackage.WORKLOAD);
		String dbname = proppkg.getProperty(PropertyPackage.DB_NAME);
		boolean dotransactions = Boolean.parseBoolean(proppkg.getProperty(PropertyPackage.DO_TRANSACTIONS));
		int opcount = Integer.parseInt(proppkg.getProperty(PropertyPackage.OP_COUNT));
		int threadcount = Integer.parseInt(proppkg.getProperty(PropertyPackage.THREAD_COUNT));
		int target = Integer.parseInt(proppkg.getProperty(PropertyPackage.TARGET));

		// compute the target throughput
		double targetperthreadperms = -1;
		if (target > 0) {
			double targetperthread = ((double) target) / ((double) threadcount);
			targetperthreadperms = targetperthread / 1000.0;
		}

		// set up measurements
		Measurements.setProperties(proppkg.props);

		// load the workload
		ClassLoader classLoader = Client.class.getClassLoader();

		try {
			Class workloadclass = classLoader.loadClass(workloadloc);
			workload = (Workload) workloadclass.newInstance();
		} catch (Exception e) {
			e.printStackTrace();
			e.printStackTrace(System.out);
			System.exit(0);
		}
		try {
			workload.init(proppkg.props);
		} catch (WorkloadException e) {
			e.printStackTrace();
			e.printStackTrace(System.out);
			System.exit(0);
		}

		// Initialize the database
		String protocol = proppkg.props.getProperty(Client.PROTOCOL_PROPERTY);
		for (int threadid = 0; threadid < threadcount; threadid++) {
			try {
				if (protocol.equals("memcached"))
					db = MemcachedFactory.newMemcached(dbname, proppkg.props);
				else if (protocol.equals("db"))
					db = DBFactory.newDB(dbname, proppkg.props);
				else {
					System.out.println("Invalid Protocol: " + protocol);
					System.exit(0);
				}
			} catch (UnknownDataStoreException e) {
				System.out.println("Unknown DB " + dbname);
				System.exit(0);
			}
			Thread t = new ClientThread(db, dotransactions, workload, threadid, threadcount, proppkg.props, opcount / threadcount, targetperthreadperms);
			threads.add(t);
		}
	}
	
	public void run() {
		int opcount = Integer.parseInt(proppkg.getProperty(PropertyPackage.OP_COUNT));
		printstatsinterval = Long.parseLong(proppkg.props.getProperty(Client.PRINT_STATS_INTERVAL, Client.PRINT_STATS_INTERVAL_DEFAULT));
		
		long st = System.currentTimeMillis();

		for (Thread t : threads) {
			t.start();
		}

		for (Thread t : threads) {
			try {
				t.join();
			} catch (InterruptedException e) {}
		}

		long en = System.currentTimeMillis();

		/*
		if (proppkg.slave) {
			statusthread.interrupt();
		}*/

		try {
			workload.cleanup();
		} catch (WorkloadException e) {
			e.printStackTrace(System.out);
			System.exit(0);
		}
		
		try {
			db.shutdown();
		} catch (DataStoreException e) {
			e.printStackTrace();
		}
	}
}
