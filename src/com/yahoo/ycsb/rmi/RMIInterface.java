package com.yahoo.ycsb.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RMIInterface extends Remote {

	public String execute() throws RemoteException;
}
