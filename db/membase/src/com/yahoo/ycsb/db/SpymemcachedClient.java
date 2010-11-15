package com.yahoo.ycsb.db;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.sql.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.protocol.TCPMemcachedNodeImpl;

import com.yahoo.ycsb.DB;


public class SpymemcachedClient extends DB {
	public static MemcachedClient client;
	public static final String VERBOSE = "memcached.verbose";
	public static final String VERBOSE_DEFAULT = "true";

	public static final String SIMULATE_DELAY = "memcached.simulatedelay";
	public static final String SIMULATE_DELAY_DEFAULT = "0";
	
	Random random;
	boolean verbose;
	int todelay;

	public SpymemcachedClient() {
		random = new Random();
		todelay = 0;
	}
	
	/**
	 * Initialize any state for this DB. Called once per DB instance; there is
	 * one DB instance per client thread.
	 */
	@SuppressWarnings("unchecked")
	public void init() {
		verbose = Boolean.parseBoolean(getProperties().getProperty(VERBOSE, VERBOSE_DEFAULT));
		todelay = Integer.parseInt(getProperties().getProperty(SIMULATE_DELAY, SIMULATE_DELAY_DEFAULT));

		String addr = getProperties().getProperty("memcached.address");
		try {
			client = new MemcachedClient(new InetSocketAddress(InetAddress.getByAddress(ipv4AddressToByte(addr)), 11211));
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		if (verbose) {
			System.out.println("***************** properties *****************");
			Properties p = getProperties();
			if (p != null) {
				for (Enumeration e = p.propertyNames(); e.hasMoreElements();) {
					String k = (String) e.nextElement();
					System.out.println("\"" + k + "\"=\"" + p.getProperty(k) + "\"");
				}
			}
			System.out.println("**********************************************");
		}
	}
	
	
	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, String> result) {
		// TODO Auto-generated method stub
		//System.out.println("Getting key: " + key);
		
		Future<Object> success = client.asyncGet(key);
		long start = System.currentTimeMillis();
			long end;
			
			/*while (!success.isDone()) {
				try {
					Thread.sleep(1);
					end = System.currentTimeMillis();
					if ((end -start) > 1000)
						System.out.println("Duration: " + (end - start) + ", Queue: " + client.conn.locator.getPrimary(key).toString());
					
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			end = System.currentTimeMillis();
			if (end - start > 1000)
				System.out.println(end - start);
		*/
		try {
			if (success.get() != null) {
				System.out.println("Error");
				return -1;
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/*System.out.println("Queue: " + client.conn.locator.getPrimary(key).toString());
		if (client.get(key) == null)
			return -1;*/
		
		return 0;
	}

	@Override
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, String>> result) {
		System.out.println("Scan");
		return 0;
	}

	@Override
	public int update(String table, String key, HashMap<String, String> values) {
		//System.out.println("Update");
		return 0; //insert(table, key, values);
	}

	@Override
	public int insert(String table, String key, HashMap<String, String> values) {
		//Set<String> keys = values.keySet();
		//Iterator<String> itr = keys.iterator();
		
		Future<Boolean> success = client.set(key, 0, values);
		
		/* -- Code to test Spymemcached 136 -- */
		long start = System.currentTimeMillis();
		long end;
		String str = "";
		while (!success.isDone()) {
			try {
				Thread.sleep(1);
				end = System.currentTimeMillis();
				if ((end - start) > 30000) {
					//System.out.println("Duration: " + (end - start) + ", Queue: " + client.conn.locator.getPrimary(key).toString());
					Date date = new Date(System.currentTimeMillis());
					
					str = key + "\n";
				}
				
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.print(str);	
		end = System.currentTimeMillis();
		if (end - start > 1000)
			System.out.println(end - start);
		
		try {
			if (!success.get().booleanValue())
				return -1;
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return 0;
	}

	@Override
	public int delete(String table, String key) {
		// TODO Auto-generated method stub
		System.out.println("Delete");
		return 0;
	}
	
	private byte[] ipv4AddressToByte(String address) {
		byte[] b = new byte[4];
		String[] str = address.split("\\.");
		b[0] = Integer.valueOf(str[0]).byteValue();
		b[1] = Integer.valueOf(str[1]).byteValue();
		b[2] = Integer.valueOf(str[2]).byteValue();
		b[3] = Integer.valueOf(str[3]).byteValue();
		return b;
	}

}
