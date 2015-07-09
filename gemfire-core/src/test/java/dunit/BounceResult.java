package dunit;

public class BounceResult {
  private final int newPid;
  private final RemoteDUnitVMIF newClient;
  
  public BounceResult(int newPid, RemoteDUnitVMIF newClient) {
    this.newPid = newPid;
    this.newClient = newClient;
  }

  public int getNewPid() {
    return newPid;
  }

  public RemoteDUnitVMIF getNewClient() {
    return newClient;
  }

}
