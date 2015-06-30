package dunit;

import hydra.MethExecutorResult;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RemoteDUnitVMIF extends Remote {

  MethExecutorResult executeMethodOnObject(Object o, String methodName) throws RemoteException;

  MethExecutorResult executeMethodOnObject(Object o, String methodName,
      Object[] args) throws RemoteException;

  MethExecutorResult executeMethodOnClass(String name, String methodName,
      Object[] args) throws RemoteException;

}
