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

package com.yahoo.ycsb.memcached;

import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.DataStoreException;
import com.yahoo.ycsb.measurements.Measurements;

/**
 * Wrapper around a "real" DB that measures latencies and counts return codes.
 */
public class MemcachedWrapper extends Memcached {
	Memcached _db;
	Measurements _measurements;

	public MemcachedWrapper(Memcached memcached) {
		_db = memcached;
		_measurements = Measurements.getMeasurements();
	}

	/**
	 * Set the properties for this Memcached.
	 */
	public void setProperties(Properties p) {
		_db.setProperties(p);
	}

	/**
	 * Get the set of properties for this Memcached.
	 */
	public Properties getProperties() {
		return _db.getProperties();
	}

	/**
	 * Initialize any state for this Memcached. Called once per Memcached instance; there is
	 * one Memcached instance per client thread.
	 */
	public void init() throws DataStoreException {
		_db.init();
	}

	/**
	 * Cleanup any state for this Memcached. Called once per Memcached instance; there is one
	 * Memcached instance per client thread.
	 */
	public void cleanup() throws DataStoreException {
		_db.cleanup();
	}

	/**
	 * Insert a record in the database. Any field/value pairs in the specified
	 * values HashMap will be written into the record with the specified record
	 * key.
	 * 
	 * @param key
	 *            The record key of the record to get.
	 * @param value
	 *            The Object that the key should contain
	 * @return Zero on success, a non-zero error code on error
	 */
	public int get(String key, Object value) {
		long st = System.nanoTime();
		int res = _db.get(key, value);
		long en = System.nanoTime();
		_measurements.measure("GET", (int) ((en - st) / 1000));
		_measurements.reportReturnCode("GET", res);
		return res;
	}
	
	/**
	 * Insert a record in the database. Any field/value pairs in the specified
	 * values HashMap will be written into the record with the specified record
	 * key.
	 * 
	 * @param key
	 *            The record key of the record to set.
	 * @param value
	 *            The Object to use as the keys value
	 * @return Zero on success, a non-zero error code on error
	 */
	public int set(String key, Object value) {
		long st = System.nanoTime();
		int res = _db.set(key, value);
		long en = System.nanoTime();
		_measurements.measure("SET", (int) ((en - st) / 1000));
		_measurements.reportReturnCode("SET", res);
		return res;
	}

}
