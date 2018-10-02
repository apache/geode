package org.apache.geode.test.dunit.standalone;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import org.apache.geode.test.dunit.VM;

class Master extends UnicastRemoteObject implements MasterRemote {
  private static final long serialVersionUID = 1178600200232603119L;

  private final Registry registry;
  private final ProcessManager processManager;


  public Master(Registry registry, ProcessManager processManager) throws RemoteException {
    this.processManager = processManager;
    this.registry = registry;
  }

  public int getLocatorPort() throws RemoteException {
    return DUnitLauncher.locatorPort;
  }

  public synchronized void signalVMReady() {
    processManager.signalVMReady();
  }

  public void ping() {
    // do nothing
  }

  @Override
  public BounceResult bounce(int pid) {
    return bounce(VersionManager.CURRENT_VERSION, pid, false);
  }

  @Override
  public BounceResult bounce(String version, int pid, boolean force) {
    processManager.bounce(version, pid, force);

    try {
      if (!processManager.waitForVMs(DUnitLauncher.STARTUP_TIMEOUT)) {
        throw new RuntimeException(DUnitLauncher.STARTUP_TIMEOUT_MESSAGE);
      }
      RemoteDUnitVMIF remote =
          (RemoteDUnitVMIF) registry.lookup(VM.getVMName(VersionManager.CURRENT_VERSION, pid));
      return new BounceResult(pid, remote);
    } catch (RemoteException | NotBoundException e) {
      throw new RuntimeException("could not lookup name", e);
    } catch (InterruptedException e) {
      throw new RuntimeException("Failed waiting for VM", e);
    }
  }
}
