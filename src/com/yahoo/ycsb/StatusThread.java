package com.yahoo.ycsb;

import java.text.DecimalFormat;
import java.util.Vector;

import com.yahoo.ycsb.measurements.Measurements;

/**
 * A thread to periodically show the status of the experiment, to reassure you
 * that progress is being made.
 * 
 * @author cooperb
 * 
 */
class StatusThread extends Thread {
	Vector<Thread> _threads;
	String _label;
	boolean _standardstatus;
	long _printstatsinterval;

	/**
	 * The interval for reporting status.
	 */
	

	public StatusThread(Vector<Thread> threads, String label,
			boolean standardstatus, long printstatsinterval) {
		_threads = threads;
		_label = label;
		_standardstatus = standardstatus;
		_printstatsinterval = printstatsinterval;
	}

	/**
	 * Run and periodically report status.
	 */
	public void run() {
		long st = System.currentTimeMillis();

		long lasten = st;
		long lasttotalops = 0;

		boolean alldone;

		do {
			alldone = true;

			int totalops = 0;

			// terminate this thread when all the worker threads are done
			for (Thread t : _threads) {
				if (t.getState() != Thread.State.TERMINATED) {
					alldone = false;
				}
				//System.out.println(t.getName() + " " + t.getState());

				ClientThread ct = (ClientThread) t;
				totalops += ct.getOpsDone();
			}

			long en = System.currentTimeMillis();

			long interval = en - st;
			// double throughput=1000.0*((double)totalops)/((double)interval);

			double curthroughput = 1000.0 * (((double) (totalops - lasttotalops)) / ((double) (en - lasten)));

			lasttotalops = totalops;
			lasten = en;

			DecimalFormat d = new DecimalFormat("#.##");

			if (totalops == 0) {
				System.err.println(_label + " " + (interval / 1000) + " sec: "
						+ totalops + " operations;"
						+ Measurements.getMeasurements().getSummary() + "\n");
			} else {
				System.err.println(_label + " " + (interval / 1000) + " sec: "
						+ totalops + " operations; " + d.format(curthroughput)
						+ " current ops/sec;"
						+ Measurements.getMeasurements().getSummary() + "\n");
			}

			if (_standardstatus) {
				if (totalops == 0) {
					System.out.println(_label + " " + (interval / 1000)
							+ " sec: " + totalops + " operations;"
							+ Measurements.getMeasurements().getSummary() + "\n");
				} else {
					System.out.println(_label + " " + (interval / 1000)
							+ " sec: " + totalops + " operations; "
							+ d.format(curthroughput) + " current ops/sec;"
							+ Measurements.getMeasurements().getSummary() + "\n");
				}
			}

			try {
				sleep(_printstatsinterval * 1000);
			} catch (InterruptedException e) {
				// do nothing
			}

		} while (!alldone);
	}
}