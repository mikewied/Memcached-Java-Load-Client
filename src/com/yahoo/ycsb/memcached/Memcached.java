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

import com.yahoo.ycsb.DataStore;

/**
 * A layer for accessing a database to be benchmarked. Each thread in the client
 * will be given its own instance of whatever DB class is to be used in the
 * test. This class should be constructed using a no-argument constructor, so we
 * can load it dynamically. Any argument-based initialization should be done by
 * init().
 * 
 * Note that YCSB does not make any use of the return codes returned by this
 * class. Instead, it keeps a count of the return values and presents them to
 * the user.
 * 
 * The semantics of methods such as insert, update and delete vary from database
 * to database. In particular, operations may or may not be durable once these
 * methods commit, and some systems may return 'success' regardless of whether
 * or not a tuple with a matching key existed before the call. Rather than
 * dictate the exact semantics of these methods, we recommend you either
 * implement them to match the database's default semantics, or the semantics of
 * your target application. For the sake of comparison between experiments we
 * also recommend you explain the semantics you chose when presenting
 * performance results.
 */
public abstract class Memcached extends DataStore{

	/**
	 * Insert a record in the database. Any field/value pairs in the specified
	 * values HashMap will be written into the record with the specified record
	 * key.
	 * 
	 * @param key
	 *            The record key of the record to get.
	 * @param value
	 *            The Object that the key should contain
	 * @return Zero on success, a non-zero error code on error. See this class's
	 *         description for a discussion of error codes.
	 */
	public abstract int get(String key, Object value);
	
	/**
	 * Insert a record in the database. Any field/value pairs in the specified
	 * values HashMap will be written into the record with the specified record
	 * key.
	 * 
	 * @param key
	 *            The record key of the record to insert.
	 * @param value
	 *            An Object to use as the key's value
	 * @return Zero on success, a non-zero error code on error. See this class's
	 *         description for a discussion of error codes.
	 */
	public abstract int set(String key, Object value);

}
