package com.yahoo.ycsb.db;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import net.spy.memcached.CASResponse;
import net.spy.memcached.CachedData;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.transcoders.Transcoder;

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
	
	public static long endtime;
	
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

		String addr = "10.2.1.11";//getProperties().getProperty("memcached.address");
		try {
			InetSocketAddress ia = new InetSocketAddress(InetAddress.getByAddress(ipv4AddressToByte(addr)), membaseport);
			client = new MemcachedClient(ia);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
	
	public void cleanup() {
		client.shutdown();
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
		Future<Object> f = client.asyncGet(key);
		//long time = System.nanoTime();
		try {
			if (f.get() == null) {
				System.out.println("Error");
				return -1;
			}
		} catch (InterruptedException e) {
			System.out.println("GET Interrupted");
		} catch (ExecutionException e) {
			System.out.println("GET Execution");
			e.printStackTrace();
		} catch (RuntimeException e) {
			System.out.println("GET Runtime");
		}
		//System.out.println("Start: " + time);
		//System.out.println("Start: " + endtime);
		//System.out.println("Spy latency: " + ((endtime - time)/1000));
		return 0;
	}
	/*
	public Future<Object> asyncGet(final String key) {
		return asyncGet(key, client.getTranscoder());
	}
	
	public <T> Future<T> asyncGet(final String key, final Transcoder<T> tc) {
		final CountDownLatch latch=new CountDownLatch(1);
		final GetFuture<T> rv=new GetFuture<T>(latch, 1000);
		
		Operation op=client.opFact.get(key,
				new GetOperation.Callback() {
			private Future<T> val=null;
			public void receivedStatus(OperationStatus status) {
				rv.set(val);
				
			}
			public void gotData(String k, int flags, byte[] data) {
				assert key.equals(k) : "Wrong key returned";
				val=client.tcService.decode(tc,
					new CachedData(flags, data, tc.getMaxSize()));
			}
			
			public void complete() {
				SpymemcachedClient.endtime = System.nanoTime();
				System.out.println("Complete");
				latch.countDown();
			}});
		rv.setOperation(op);
		client.addOp(key, op);
		return rv;
	}*/
	
	
	

	@Override
	public int set(String key, Object value) {
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
	
	public static void main(String args[]) {
		SpymemcachedClient client = new SpymemcachedClient();
		client.init();
	}
}
