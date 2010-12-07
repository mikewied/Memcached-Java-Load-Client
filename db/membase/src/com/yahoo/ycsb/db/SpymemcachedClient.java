package com.yahoo.ycsb.db;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import net.spy.memcached.CASResponse;
import net.spy.memcached.MemcachedClient;

import com.yahoo.ycsb.memcached.Memcached;


public class SpymemcachedClient extends Memcached {
	public static MemcachedClient client;
	public static final String VERBOSE = "memcached.verbose";
	public static final String VERBOSE_DEFAULT = "true";

	public static final String SIMULATE_DELAY = "memcached.simulatedelay";
	public static final String SIMULATE_DELAY_DEFAULT = "0";
	
	public static final String MEMBASE_PORT = "membase.port";
	public static final String MEMBASE_PORT_DEFAULT = "11211";
	int membaseport;
	
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
	public void init() {
		verbose = Boolean.parseBoolean(getProperties().getProperty(VERBOSE, VERBOSE_DEFAULT));
		todelay = Integer.parseInt(getProperties().getProperty(SIMULATE_DELAY, SIMULATE_DELAY_DEFAULT));
		membaseport = Integer.parseInt(getProperties().getProperty(MEMBASE_PORT, MEMBASE_PORT_DEFAULT));

		String addr = getProperties().getProperty("memcached.address");
		try {
			InetSocketAddress ia = new InetSocketAddress(InetAddress.getByAddress(ipv4AddressToByte(addr)), membaseport);
			client = new MemcachedClient(ia);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
	
	@Override
	public int add(String key, Object value) {
		try {
			if (!client.add(key, 0, value).get().booleanValue())
				return -1;
		} catch (InterruptedException e) {
			System.out.println("ADD Interrupted");
		} catch (ExecutionException e) {
			System.out.println("ADD Execution");
		} catch (RuntimeException e) {
			System.out.println("ADD Runtime");
		}
		return 0;
	}
	
	@Override
	public int get(String key, Object value) {	
		try {
			if (client.asyncGet(key).get() == null) {
				System.out.println("Error");
				return -1;
			}
		} catch (InterruptedException e) {
			System.out.println("GET Interrupted");
		} catch (ExecutionException e) {
			System.out.println("GET Execution");
			//e.printStackTrace();
		} catch (RuntimeException e) {
			System.out.println("GET Runtime");
		}
		return 0;
	}
	
	@Override
	public int set(String key, Object value) {
		try {
			if (!client.set(key, 0, value).get().booleanValue())
				return -1;
		} catch (InterruptedException e) {
			System.out.println("SET Interrupted");
		} catch (ExecutionException e) {
			System.out.println("SET Execution");
			//e.printStackTrace();
		} catch (RuntimeException e) {
			System.out.println("SET Runtime");
		}
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

	@Override
	public int append(String key, long cas, Object value) {
		try {
			if (!client.append(cas, key, value).get().booleanValue())
				return -1;
		} catch (InterruptedException e) {
			System.out.println("APPEND Interrupted");
		} catch (ExecutionException e) {
			System.out.println("APPEND Execution");
		} catch (RuntimeException e) {
			System.out.println("APPEND Runtime");
		}
		return 0;
	}

	@Override
	public int cas(String key, long cas, Object value) {
		if (!client.cas(key, cas, value).equals(CASResponse.OK))
			return -1;
		return 0;
	}

	@Override
	public int decr(String key, Object value) {
		return 0;
	}

	@Override
	public int delete(String key) {
		return 0;
	}

	@Override
	public int incr(String key, Object value) {
		return 0;
	}

	@Override
	public long gets(String key) {
		long cas = client.gets(key).getCas();
		if (cas < 0)
			return -1;
		return cas;
	}

	@Override
	public int prepend(String key, long cas, Object value) {
		try {
			if (!client.prepend(cas, key, value).get().booleanValue())
				return -1;
		} catch (InterruptedException e) {
			System.out.println("PREPEND Interrupted");
		} catch (ExecutionException e) {
			System.out.println("PREPEND Execution");
		} catch (RuntimeException e) {
			System.out.println("PREPEND Runtime");
		}
		return 0;
	}

	@Override
	public int replace(String key, Object value) {
		try {
			if (!client.replace(key, 0, value).get().booleanValue())
				return -1;
		} catch (InterruptedException e) {
			System.out.println("REPLACE Interrupted");
		} catch (ExecutionException e) {
			System.out.println("REPLACE Execution");
		} catch (RuntimeException e) {
			System.out.println("REPLACE Runtime");
		}
		return 0;
	}
}
