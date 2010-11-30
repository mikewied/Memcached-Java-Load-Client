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

import java.util.Properties;

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

	@Override
	public int add(String key, Object value) {
		long st = System.nanoTime();
		int res = _db.add(key, value);
		long en = System.nanoTime();
		_measurements.measure("ADD", (int) ((en - st) / 1000));
		_measurements.reportReturnCode("ADD", res);
		return res;
	}

	@Override
	public int append(String key, long cas, Object value) {
		long st = System.nanoTime();
		int res = _db.append(key, cas, value);
		long en = System.nanoTime();
		_measurements.measure("APPEND", (int) ((en - st) / 1000));
		_measurements.reportReturnCode("APPEND", res);
		return res;
	}

	@Override
	public int cas(String key, Object value) {
		long st = System.nanoTime();
		int res = _db.cas(key, value);
		long en = System.nanoTime();
		_measurements.measure("CAS", (int) ((en - st) / 1000));
		_measurements.reportReturnCode("CAS", res);
		return res;
	}

	@Override
	public int decr(String key, Object value) {
		long st = System.nanoTime();
		int res = _db.decr(key, value);
		long en = System.nanoTime();
		_measurements.measure("DECR", (int) ((en - st) / 1000));
		_measurements.reportReturnCode("DECR", res);
		return res;
	}

	@Override
	public int delete(String key) {
		long st = System.nanoTime();
		int res = _db.delete(key);
		long en = System.nanoTime();
		_measurements.measure("DELETE", (int) ((en - st) / 1000));
		_measurements.reportReturnCode("DELETE", res);
		return res;
	}

	@Override
	public int incr(String key, Object value) {
		long st = System.nanoTime();
		int res = _db.incr(key, value);
		long en = System.nanoTime();
		_measurements.measure("INCR", (int) ((en - st) / 1000));
		_measurements.reportReturnCode("INCR", res);
		return res;
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

	@Override
	public long gets(String key) {
		long st = System.nanoTime();
		long res = _db.gets(key);
		long en = System.nanoTime();
		_measurements.measure("GETS", (int) ((en - st) / 1000));
		if (res > 0)
			_measurements.reportReturnCode("GETS", 0);
		else
			_measurements.reportReturnCode("GETS", -1);
		return res;
	}

	@Override
	public int prepend(String key, long cas, Object value) {
		long st = System.nanoTime();
		int res = _db.prepend(key, cas, value);
		long en = System.nanoTime();
		_measurements.measure("PREPEND", (int) ((en - st) / 1000));
		_measurements.reportReturnCode("PREPEND", 0);
		return res;
	}

	@Override
	public int replace(String key, Object value) {
		long st = System.nanoTime();
		int res = _db.replace(key, value);
		long en = System.nanoTime();
		_measurements.measure("REPLACE", (int) ((en - st) / 1000));
		_measurements.reportReturnCode("REPLACE", res);
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
