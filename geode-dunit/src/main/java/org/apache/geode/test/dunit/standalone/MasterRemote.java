package org.apache.geode.test.dunit.standalone;

import java.rmi.Remote;
import java.rmi.RemoteException;

interface MasterRemote extends Remote {
  int getLocatorPort() throws RemoteException;

  void signalVMReady() throws RemoteException;

  void ping() throws RemoteException;

  BounceResult bounce(int pid) throws RemoteException;

  BounceResult bounce(String version, int pid, boolean force) throws RemoteException;
}
