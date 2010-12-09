package com.yahoo.ycsb.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RMIInterface extends Remote {

	public int execute() throws RemoteException;
	
	public int setProperties(PropertyPackage proppkg) throws RemoteException;
}
