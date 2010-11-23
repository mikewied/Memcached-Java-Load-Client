package com.yahoo.ycsb.db;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import net.spy.memcached.MemcachedClient;

import com.yahoo.ycsb.memcached.Memcached;


public class SpymemcachedClient extends Memcached {
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
	@SuppressWarnings("rawtypes")
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
	public int get(String key, Object value) {
		//System.out.println("GET " + key);
		Future<Object> success = client.asyncGet(key);		
		try {
			if (success.get() == null) {
				System.out.println("Error");
				return -1;
			}
		} catch (InterruptedException e) {
			System.out.println("GET Interrupted");
		} catch (ExecutionException e) {
			System.out.println("GET Execution");
		} catch (RuntimeException e) {
			System.out.println("GET Runtime");
		}
		return 0;
	}
	
	@Override
	public int set(String key, Object value) {
		//System.out.println("SET " + key);
		try {
			if (!client.set(key, 0, value).get().booleanValue())
				return -1;
		} catch (InterruptedException e) {
			System.out.println("SET Interrupted");
		} catch (ExecutionException e) {
			System.out.println("SET Execution");
		} catch (RuntimeException e) {
			System.out.println("SET Runtime");
		}
		return 0;
	}
/*
	@Override
	public int delete(String table, String key) {
		// TODO Auto-generated method stub
		System.out.println("Delete");
		return 0;
	}
	*/
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
