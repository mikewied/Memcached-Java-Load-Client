package com.yahoo.ycsb.measurements;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import com.yahoo.ycsb.client.Client;
import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

public class Measurements {
	private static final long serialVersionUID = -311232927139188477L;

	private static final String MEASUREMENT_TYPE = "measurementtype";

	private static final String MEASUREMENT_TYPE_DEFAULT = "histogram";

	static Measurements measurements = null;

	static Properties measurementproperties = null;

	public static void setProperties(Properties props) {
		measurementproperties = props;
	}

	/**
	 * Return the singleton Measurements object.
	 */
	public synchronized static Measurements getMeasurements() {
		if (measurements == null)
			measurements = new Measurements(measurementproperties);
		return measurements;
	}

	private long operations;
	private int partialoperations;
	HashMap<String, OneMeasurement> totaldata;
	HashMap<String, OneMeasurement> partialdata;
	boolean histogram = true;

	private Properties _props;

	/**
	 * Create a new object with the specified properties.
	 */
	public Measurements(Properties props) {
		totaldata = new HashMap<String, OneMeasurement>();
		partialdata = new HashMap<String, OneMeasurement>();

		_props = props;
		operations = 0;

		if (_props.getProperty(MEASUREMENT_TYPE, MEASUREMENT_TYPE_DEFAULT)
				.compareTo("histogram") == 0) {
			histogram = true;
		} else {
			histogram = false;
		}
	}

	OneMeasurement constructOneMeasurement(String name) {
		if (histogram) {
			return new OneMeasurementHistogram(name, _props);
		} else {
			return new OneMeasurementTimeSeries(name, _props);
		}
	}

	/**
	 * Report a single value of a single metric. E.g. for read latency,
	 * operation="READ" and latency is the measured value.
	 */
	public synchronized void measure(String operation, int latency) {
		if (!totaldata.containsKey(operation) || !partialdata.containsKey(operation)) {
			synchronized (this) {
				if (!totaldata.containsKey(operation)) {
					totaldata.put(operation, constructOneMeasurement(operation));
				}
				if (!partialdata.containsKey(operation)) {
					partialdata.put(operation, constructOneMeasurement(operation));
				}
			}
		}
		try {
			operations++;
			partialoperations++;
			totaldata.get(operation).measure(latency);
			partialdata.get(operation).measure(latency);
		} catch (java.lang.ArrayIndexOutOfBoundsException e) {
			System.out.println("ERROR: java.lang.ArrayIndexOutOfBoundsException - ignoring and continuing");
			e.printStackTrace();
			e.printStackTrace(System.out);
		}
	}
	
	public synchronized void add(HashMap<String, OneMeasurement> m) {
		if (m != null) {
			if (m.size() > 0) {
				synchronized (this) {
					Set<String> keyset = this.totaldata.keySet();
					Iterator<String> itr = keyset.iterator();
					
					while (itr.hasNext()) {
						String item = itr.next();
						this.operations += m.get(item).getOperations();
						this.partialoperations += m.get(item).getOperations();
						this.totaldata.get(item).add(m.get(item));
						this.partialdata.get(item).add(m.get(item));
					}
				}
			}
		}
	}
	
	public synchronized HashMap<String, OneMeasurement> getPartialData() {
		if (partialdata == null)
			System.out.println("Partial Data is NULL");
		return partialdata;
	}
	
	public synchronized HashMap<String, OneMeasurement> getAndResetPartialData() {
		HashMap<String, OneMeasurement> m = partialdata;
		partialdata = new HashMap<String, OneMeasurement>();
		return m;
	}

	/**
	 * Report a return code for a single DB operaiton.
	 */
	public void reportReturnCode(String operation, int code) {
		if (!totaldata.containsKey(operation)) {
			synchronized (this) {
				if (!totaldata.containsKey(operation)) {
					totaldata.put(operation, constructOneMeasurement(operation));
				}
			}
		}
		totaldata.get(operation).reportReturnCode(code);
	}

	/**
	 * Export the current measurements to a suitable format.
	 * 
	 * @param exporter
	 *            Exporter representing the type of format to write to.
	 * @throws IOException
	 *             Thrown if the export failed.
	 */
	public void exportMeasurements(MeasurementsExporter exporter)
			throws IOException {
		for (OneMeasurement measurement : totaldata.values()) {
			measurement.exportMeasurements(exporter);
		}
	}

	/**
	 * Return a one line summary of the measurements.
	 */
	public synchronized String getSummary() {
		int interval = Integer.parseInt(_props.getProperty(Client.PRINT_STATS_INTERVAL, Client.PRINT_STATS_INTERVAL_DEFAULT));
		
		String ret = " " + operations + " operations; " + (partialoperations / interval) + " ops/sec";
		for (OneMeasurement m : partialdata.values()) {
			ret += m.getSummary() + " ";
		}
		partialoperations = 0;
		getAndResetPartialData();
		return ret;
	}
	
	public long getOperations() {
		return operations;
	}
}
