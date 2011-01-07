package com.yahoo.ycsb.client;

public class ControlClient {
	private static final String WKSET = "wkset";
	private static final String THROUGHPUT = "throughput";
	private static final String INCITEMS = "incitems";
	private static final String SIZE_ARG = "-s";
	private static final String TIME_ARG = "-t";
	private static final String CHANGE_ARG = "-c";

	public ControlClient() {
		
	}
	
	public void changeWorkingSet(int size) {
		System.out.println("Changed Working Set to " + size);
	}
	
	public void changeThroughput(int size) {
		System.out.println("Changed Throughput to " + size);
	}
	
	public void incrItems(int change, int time, int size) {
		System.out.println("Increasing Items at interval " + change + " every " + time + " seconds until " + size + " items");
	}
	
	public static void main(String args[]) {
		ControlClient client = new ControlClient();
		int argindex = 0;
		
		if (args.length == 0) {
			ControlClient.usage();
			System.exit(0);
		}
		
		if (args[argindex].equals(WKSET)) {
			try {
				if (args[++argindex].equals(SIZE_ARG))
					client.changeWorkingSet(parseIntegerArg(args, ++argindex));
			} catch (IndexOutOfBoundsException e) {
				System.out.println("usage: wkset -s [SIZE]");
			}
		} else if (args[argindex].equals(THROUGHPUT)) {
			try {
				if (args[++argindex].equals(SIZE_ARG))
					client.changeThroughput(parseIntegerArg(args, ++argindex));
			} catch (IndexOutOfBoundsException e) {
				System.out.println("usage: throughput -s [SIZE]");
			}
		} else if (args[argindex].equals(INCITEMS)) {
			int size = -1;
			int time = -1;
			int change = 0;
			while(++argindex < args.length) {
				try {
					if (args[argindex].equals(CHANGE_ARG))
						change = parseIntegerArg(args, ++argindex);
					else if (args[argindex].equals(TIME_ARG))
						time = parseIntegerArg(args, ++argindex);
					else if (args[argindex].equals(SIZE_ARG))
						size = parseIntegerArg(args, ++argindex);
					else
						System.out.println("Invalid option " + args[argindex]);
				} catch (IndexOutOfBoundsException e) {
					System.out.println(argindex + " " + args.length);
					System.out.println("usage: incritems -c [CHANGE] -t [TIME] -s [SIZE]");
				}
			}
			if (size != -1 && time != -1 && change != 0)
				client.incrItems(change, time, size);
			else {
				System.out.println("Not all flags defined or change is 0");
				System.out.println("usage: incitems -c [CHANGE] -t [TIME]  -s [SIZE]");
			}
		} else {
			System.out.println("Invalid OPER " + args[argindex]);
			usage();
		}
			
	}
	
	public static int parseIntegerArg(String args[], int argindex) {
		try {
			int size = Integer.parseInt(args[argindex]);
			return size;
		} catch (IndexOutOfBoundsException e) {
			System.out.println("No argument found for size flag");
		} catch (NumberFormatException e) {
			System.out.println(args[argindex] + " is not a valid integer");
		}
		System.exit(0);
		return -1; // We will never get here
	}
	
	private static void usage() {
		System.out.println("usage: java com.yahoo.ycsb.client.ControlClient OPER [OPTIONS]\n");
		System.out.println("OPER is [wkset, throughput, incitems]\n");
		System.out.println("Commands include:");
		System.out.println("wkset -s [size]");
		System.out.println("\t[size] is the new size of the working set");
		System.out.println("throughput -s [size]");
		System.out.println("\t[size] is the max amount of operations per second");
		System.out.println("incitems -c [change] -t [time] -s [size]");
		System.out.println("\t[change] is the amount of items to add in a given time interval");
		System.out.println("\t[time] is the time interval (in seconds)");
		System.out.println("\t[size] is the amount of items in the db when this operation terminates");
	}
}
