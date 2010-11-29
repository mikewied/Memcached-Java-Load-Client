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

package com.yahoo.ycsb.generator;

import java.util.Random;

/**
 * Generate a popularity distribution of items, skewed to favor recent items
 * significantly more than older items.
 */
public class ChurnGenerator extends IntegerGenerator {
	Random _r;
	int workingsetsize;
	int workingsetdelta;
	int workingSetLo;
	int workingSetHi;
	int ops;

	public ChurnGenerator(int workingsetsize, int workingsetdelta) {
		_r = new Random();
		this.workingsetsize = workingsetsize;
		this.workingsetdelta = workingsetdelta;
		workingSetLo = 0;
		workingSetHi = workingsetsize;
		ops = 0;
		nextInt();
	}

	/**
	 * Generate the next string in the distribution, skewed Zipfian favoring the
	 * items most recently returned by the basis generator.
	 */
	public int nextInt() {
		if (ops > workingsetdelta) {
			ops = 0;
			workingSetLo++;
			workingSetHi++;
		}
		int relvalue = _r.nextInt(workingsetsize);
		ops++;
		return (workingSetLo + relvalue) % 50;
	}

	public static void main(String[] args) {
		int[] keys = new int[50];
		ChurnGenerator gen = new ChurnGenerator(5, 1000);
		for (int i = 0; i < Integer.parseInt(args[0]); i++) {
			keys[Integer.parseInt(gen.nextString())]++;
		}
		for (int i = 0; i < keys.length; i++) {
			System.out.println(i + ": " + keys[i]);
		}
	}
}
