package com.yahoo.ycsb.client;

public interface ClientStatus {
	static int NOT_STARTED = 0;
	static int READY = 1;
	static int RUNNING = 2;
	static int DONE = 3;
	static int TERMINATED = 4;
	
	public int getStatus();
}
