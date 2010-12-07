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

package com.yahoo.ycsb.measurements;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

/**
 * Take measurements and maintain a histogram of a given metric, such as READ
 * LATENCY.
 * 
 * @author cooperb
 *
 */
public class OneMeasurementHistogramCustom extends OneMeasurement {
	public static final int BUCKETS = 20;
	
	int[] histogram;
	int[] windowhistogram;
	int histogramoverflow;
	int operations;
	long totallatency;

	// keep a windowed version of these stats for printing status
	int windowoperations;
	long windowtotallatency;
	int exp_offset;
	double stddev_pts;
	int min;
	int max;
	HashMap<Integer, int[]> returncodes;

	public OneMeasurementHistogramCustom(String name, Properties props) {
		super(name);
		histogram = new int[BUCKETS];
		windowhistogram = new int[BUCKETS];
		histogramoverflow = 0;
		operations = 0;
		totallatency = 0;
		windowoperations = 0;
		windowtotallatency = 0;
		exp_offset = 8;
		stddev_pts = 0;
		min = -1;
		max = -1;
		returncodes = new HashMap<Integer, int[]>();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.yahoo.ycsb.OneMeasurement#reportReturnCode(int)
	 */
	public synchronized void reportReturnCode(int code) {
		Integer Icode = code;
		if (!returncodes.containsKey(Icode)) {
			int[] val = new int[1];
			val[0] = 0;
			returncodes.put(Icode, val);
		}
		returncodes.get(Icode)[0]++;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.yahoo.ycsb.OneMeasurement#measure(int)
	 */
	public synchronized void measure(int latency) {
		if (((int)Math.pow(2.0, (double)(BUCKETS - 1)) < latency))
			histogramoverflow++;
		
		for (int i = 0; i < BUCKETS - 1; i++) {
			if (latency < Math.pow(2.0, (double)(i + exp_offset))) {
				histogram[i]++;
				windowhistogram[i]++;
				break;
			}
		}
		
		operations++;
		totallatency += latency;
		windowoperations++;
		windowtotallatency += latency;
		
		double mean = totallatency / operations;
		stddev_pts += Math.pow((latency - mean), 2.0);

		if ((min < 0) || (latency < min)) {
			min = latency;
		}

		if ((max < 0) || (latency > max)) {
			max = latency;
		}
	}

	@Override
	public void exportMeasurements(MeasurementsExporter exporter) throws IOException {
		double mean = (((double) totallatency) / ((double) operations));
		double stddev = Math.sqrt(stddev_pts/operations);
		exporter.write(getName(), "Operations", operations);
		exporter.write(getName(), "AverageLatency(us)", mean);
		exporter.write(getName(), "Standard Deviation", stddev);
		exporter.write(getName(), "MinLatency(us)", min);
		exporter.write(getName(), "MaxLatency(us)", max);
		exporter.write(getName(), "95thPercentileLatency(us)", getPercentile(histogram, .95));
		exporter.write(getName(), "99thPercentileLatency(us)", getPercentile(histogram, .99));
		exporter.write(getName(), "99.9thPercentileLatency(us)", getPercentile(histogram, .999));
		
		for (Integer I : returncodes.keySet()) {
			int[] val = returncodes.get(I);
			exporter.write(getName(), "Return=" + I, val[0]);
		}

		String lower_bound;
		String upper_bound;
		for (int i = 0; i < BUCKETS; i++) {
			if (i == 0)
				lower_bound = computeTime(0);
			else
				lower_bound = computeTime((int)Math.pow(2.0, (double)(i + exp_offset - 1)));
			upper_bound = computeTime((int)Math.pow(2.0, (double)(i + exp_offset)));
			exporter.write(getName(), (lower_bound + " - " + upper_bound), histogram[i]);
			
		}
		String overflowtime = computeTime((int)Math.pow(2.0, (double)((BUCKETS + exp_offset - 1))));
		exporter.write(getName(), ">" + overflowtime, histogramoverflow);
	}
	
	@Override
	public String getSummary() {
		if (windowoperations == 0) {
			return "";
		}
		String avg = computeTime((int)(((double) windowtotallatency) / ((double) windowoperations)));
		String p99 = computeTime((int)getPercentile(windowhistogram, .99));
		windowtotallatency = 0;
		windowoperations = 0;
		for (int i = 0; i < BUCKETS; i++)
			windowhistogram[i] = 0;
		
		return "[" + getName() + " avg=" + avg +
			" 99th=" + p99 + "]";
	}
	
	public String computeTime(int time) {
		int i;
		for (i = 0; time > 1024 && i < 2; i++)
			time = time / 1024;
		
		if (i == 0)
			return String.format("%-6s", (Integer.toString(time) + "us"));
		else if (i == 1)
			return String.format("%-6s", (Integer.toString(time) + "ms"));
		else
			return String.format("%-6s", (Integer.toString(time) + "s"));
		
	}
	
	public double getPercentile(int[] data, double percentile) {
		int i;
		int opcounter = 0;
		for (i = 0; i < BUCKETS; i++) {
			opcounter += data[i];
			if (((double) opcounter) / ((double) windowoperations) >= percentile) {
				break;
			}
		}
		return (Math.pow(2, (i + 1 + exp_offset)));
	}

}
