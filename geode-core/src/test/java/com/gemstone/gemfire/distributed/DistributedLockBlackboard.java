package com.gemstone.gemfire.distributed;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Created by bschuchardt on 5/9/2016.
 */
public interface DistributedLockBlackboard extends Remote, Serializable {
  void initCount() throws RemoteException;

  void incCount() throws RemoteException;

  long getCount() throws RemoteException;

  void setIsLocked(boolean isLocked) throws RemoteException;

  boolean getIsLocked() throws RemoteException;
}
