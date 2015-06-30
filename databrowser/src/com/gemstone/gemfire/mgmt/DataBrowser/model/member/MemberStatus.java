package com.gemstone.gemfire.mgmt.DataBrowser.model.member;

import java.net.InetAddress;

public class MemberStatus {

  private String memberName;
  private String bindAddress;
  private InetAddress hostAddress;
  private int serverPort;
  public String getMemberName() {
    return memberName;
  }
  public void setMemberName(String memberName) {
    this.memberName = memberName;
  }
  public String getBindAddress() {
    return bindAddress;
  }
  public void setBindAddress(String bindAddress) {
    this.bindAddress = bindAddress;
  }
  public InetAddress getHostAddress() {
    return hostAddress;
  }
  public void setHostAddress(InetAddress hostAddress) {
    this.hostAddress = hostAddress;
  }
  public int getServerPort() {
    return serverPort;
  }
  public void setServerPort(int serverPort) {
    this.serverPort = serverPort;
  }
  
  
}
