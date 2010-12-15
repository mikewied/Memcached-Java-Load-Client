package com.yahoo.ycsb.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;

import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.measurements.OneMeasurement;

public interface RMIInterface extends Remote {

	public int execute() throws RemoteException;
	
	public int getStatus() throws RemoteException;
	
	public HashMap<String, OneMeasurement> getCurrentStats() throws RemoteException;
	
	public int setProperties(PropertyPackage proppkg) throws RemoteException;
}
