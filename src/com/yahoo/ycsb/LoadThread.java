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

package com.yahoo.ycsb;

import java.io.*;
import java.util.*;

import com.yahoo.ycsb.database.DBFactory;
import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;
import com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter;
import com.yahoo.ycsb.memcached.MemcachedFactory;
//import org.apache.log4j.BasicConfigurator;

/**
 * Main class for executing YCSB.
 */
public class LoadThread extends Thread {


	public static final String PRINT_STATS_INTERVAL_DEFAULT = "5";
	static long printstatsinterval;
	
	
	String dbname;
	Properties props;
	boolean dotransactions;
	int threadcount;
	int target;
	boolean status;
	String label;
	
	public LoadThread(Properties props, boolean dotransactions, int threadcount, int target, boolean status, String label) {
		this.props = props;
		this.dotransactions = dotransactions;
		this.threadcount = threadcount;
		this.target = target;
		this.status = status;
		this.label = label;
	}
		

	public void run() {	

		// get number of threads, target and db
		threadcount = Integer.parseInt(props.getProperty("threadcount", "1"));
		dbname = props.getProperty("db", "com.yahoo.ycsb.BasicDB");
		target = Integer.parseInt(props.getProperty("target", "0"));

		// compute the target throughput
		double targetperthreadperms = -1;
		if (target > 0) {
			double targetperthread = ((double) target) / ((double) threadcount);
			targetperthreadperms = targetperthread / 1000.0;
		}

		// show a warning message that creating the workload is taking a while
		// but only do so if it is taking longer than 2 seconds
		// (showing the message right away if the setup wasn't taking very long
		// was confusing people)
		Thread warningthread = new Thread() {
			public void run() {
				try {
					sleep(2000);
				} catch (InterruptedException e) {
					return;
				}
				System.err.println(" (might take a few minutes for large data sets)");
			}
		};

		warningthread.start();

		// set up measurements
		Measurements.setProperties(props);

		// load the workload
		ClassLoader classLoader = Client.class.getClassLoader();

		Workload workload = null;

		try {
			Class workloadclass = classLoader.loadClass(props.getProperty(Client.WORKLOAD_PROPERTY));

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

		warningthread.interrupt();

		// run the workload

		System.err.println("Starting test.");

		int opcount;
		if (dotransactions) {
			opcount = Integer.parseInt(props.getProperty(Client.OPERATION_COUNT_PROPERTY, "0"));
		} else {
			if (props.containsKey(Client.INSERT_COUNT_PROPERTY)) {
				opcount = Integer.parseInt(props.getProperty(Client.INSERT_COUNT_PROPERTY, "0"));
			} else {
				opcount = Integer.parseInt(props.getProperty(Client.RECORD_COUNT_PROPERTY, "0"));
			}
		}

		Vector<Thread> threads = new Vector<Thread>();

		String protocol = props.getProperty(Client.PROTOCOL_PROPERTY);
		for (int threadid = 0; threadid < threadcount; threadid++) {
			DataStore db = null;
			try {
				if (protocol.equals("memcached"))
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
			Thread t = new ClientThread(db, dotransactions, workload, threadid,
					threadcount, props, opcount / threadcount,
					targetperthreadperms);

			threads.add(t);
			// t.start();
		}
		
		printstatsinterval = Long.parseLong(props.getProperty(Client.PRINT_STATS_INTERVAL, PRINT_STATS_INTERVAL_DEFAULT));
		StatusThread statusthread = null;

		if (status) {
			boolean standardstatus = false;
			if (props.getProperty("measurementtype", "")
					.compareTo("timeseries") == 0) {
				standardstatus = true;
			}
			statusthread = new StatusThread(threads, label, standardstatus, printstatsinterval);
			statusthread.start();
		}

		long st = System.currentTimeMillis();

		for (Thread t : threads) {
			t.start();
		}

		for (Thread t : threads) {
			try {
				t.join();
			} catch (InterruptedException e) {
			}
		}

		long en = System.currentTimeMillis();

		if (status) {
			statusthread.interrupt();
		}

		try {
			workload.cleanup();
		} catch (WorkloadException e) {
			e.printStackTrace();
			e.printStackTrace(System.out);
			System.exit(0);
		}

		try {
			exportMeasurements(props, opcount, en - st);
		} catch (IOException e) {
			System.err.println("Could not export measurements, error: "
					+ e.getMessage());
			e.printStackTrace();
			System.exit(-1);
		}
		System.out.println("Thread ending");
	}
	
	public void printStartupMessage(String[] args) {
		System.out.println("YCSB Client 0.1");
		System.out.print("Command line:");
		for (int i = 0; i < args.length; i++) {
			System.out.print(" " + args[i]);
		}
		System.out.println();
		System.err.println("Loading workload...");
	}

	/**
	 * Exports the measurements to either sysout or a file using the exporter
	 * loaded from conf.
	 * 
	 * @throws IOException
	 *             Either failed to write to output stream or failed to close
	 *             it.
	 */
	private void exportMeasurements(Properties props, int opcount, long runtime) throws IOException {
		MeasurementsExporter exporter = null;
		try {
			// if no destination file is provided the results will be written to stdout
			OutputStream out;
			String exportFile = props.getProperty("exportfile");
			if (exportFile == null) {
				out = System.out;
			} else {
				out = new FileOutputStream(exportFile);
			}

			// if no exporter is provided the default text one will be used
			String exporterStr = props.getProperty("exporter", "com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter");
			try {
				exporter = (MeasurementsExporter) Class.forName(exporterStr)
						.getConstructor(OutputStream.class).newInstance(out);
			} catch (Exception e) {
				System.err.println("Could not find exporter " + exporterStr + ", will use default text reporter.");
				e.printStackTrace();
				exporter = new TextMeasurementsExporter(out);
			}

			exporter.write("OVERALL", "RunTime(ms)", runtime);
			double throughput = 1000.0 * ((double) opcount) / ((double) runtime);
			exporter.write("OVERALL", "Throughput(ops/sec)", throughput);

			Measurements.getMeasurements().exportMeasurements(exporter);
		} finally {
			if (exporter != null) {
				exporter.close();
			}
		}
	}
	

	
	
}
