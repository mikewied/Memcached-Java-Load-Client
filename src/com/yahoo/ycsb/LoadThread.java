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
import com.yahoo.ycsb.rmi.PropertyPackage;

/**
 * Main class for executing YCSB.
 */
public class LoadThread extends Thread {


	public static final String PRINT_STATS_INTERVAL_DEFAULT = "5";
	static long printstatsinterval;
	PropertyPackage proppkg;
	
	public LoadThread(PropertyPackage proppkg) {
		this.proppkg = proppkg;
	}
		

	public void run() {	

		// get number of threads, target and db
		proppkg.threadcount = Integer.parseInt(proppkg.props.getProperty("threadcount", "1"));
		proppkg.dbname = proppkg.props.getProperty("db", "com.yahoo.ycsb.BasicDB");
		proppkg.target = Integer.parseInt(proppkg.props.getProperty("target", "0"));

		// compute the target throughput
		double targetperthreadperms = -1;
		if (proppkg.target > 0) {
			double targetperthread = ((double) proppkg.target) / ((double) proppkg.threadcount);
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
		Measurements.setProperties(proppkg.props);

		// load the workload
		ClassLoader classLoader = Client.class.getClassLoader();

		Workload workload = null;

		try {
			Class workloadclass = classLoader.loadClass(proppkg.props.getProperty(Client.WORKLOAD_PROPERTY));

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

		warningthread.interrupt();

		// run the workload

		System.err.println("Starting test.");

		int opcount;
		if (proppkg.dotransactions) {
			opcount = Integer.parseInt(proppkg.props.getProperty(Client.OPERATION_COUNT_PROPERTY, "0"));
		} else {
			if (proppkg.props.containsKey(Client.INSERT_COUNT_PROPERTY)) {
				opcount = Integer.parseInt(proppkg.props.getProperty(Client.INSERT_COUNT_PROPERTY, "0"));
			} else {
				opcount = Integer.parseInt(proppkg.props.getProperty(Client.RECORD_COUNT_PROPERTY, "0"));
			}
		}

		Vector<Thread> threads = new Vector<Thread>();

		String protocol = proppkg.props.getProperty(Client.PROTOCOL_PROPERTY);
		for (int threadid = 0; threadid < proppkg.threadcount; threadid++) {
			DataStore db = null;
			try {
				if (protocol.equals("memcached"))
					db = MemcachedFactory.newMemcached(proppkg.dbname, proppkg.props);
				else if (protocol.equals("db"))
					db = DBFactory.newDB(proppkg.dbname, proppkg.props);
				else {
					System.out.println("Invalid Protocol: " + protocol);
					System.exit(0);
				}
			} catch (UnknownDataStoreException e) {
				System.out.println("Unknown DB " + proppkg.dbname);
				System.exit(0);
			}
			Thread t = new ClientThread(db, proppkg.dotransactions, workload, threadid,
					proppkg.threadcount, proppkg.props, opcount / proppkg.threadcount,
					targetperthreadperms);

			threads.add(t);
			// t.start();
		}
		
		printstatsinterval = Long.parseLong(proppkg.props.getProperty(Client.PRINT_STATS_INTERVAL, PRINT_STATS_INTERVAL_DEFAULT));
		StatusThread statusthread = null;

		if (proppkg.status) {
			boolean standardstatus = false;
			if (proppkg.props.getProperty("measurementtype", "")
					.compareTo("timeseries") == 0) {
				standardstatus = true;
			}
			statusthread = new StatusThread(threads, proppkg.label, standardstatus, printstatsinterval);
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

		if (proppkg.status) {
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
			exportMeasurements(proppkg.props, opcount, en - st);
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
