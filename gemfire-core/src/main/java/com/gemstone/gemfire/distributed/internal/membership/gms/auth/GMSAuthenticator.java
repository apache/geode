package com.gemstone.gemfire.distributed.internal.membership.gms.auth;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.distributed.internal.membership.gms.GMSMemberServices;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.Authenticator;
import com.gemstone.gemfire.security.AuthenticationFailedException;

public class GMSAuthenticator implements Authenticator {

  @Override
  public void init(GMSMemberServices s) {
    // TODO Auto-generated method stub

  }

  @Override
  public void start() {
    // TODO Auto-generated method stub

  }

  @Override
  public void started() {
    // TODO Auto-generated method stub

  }

  @Override
  public void stop() {
    // TODO Auto-generated method stub

  }

  @Override
  public void stopped() {
    // TODO Auto-generated method stub

  }

  @Override
  public void installView(NetView v) {
    // TODO Auto-generated method stub

  }

  @Override
  public void beSick() {
    // TODO Auto-generated method stub

  }

  @Override
  public void playDead() {
    // TODO Auto-generated method stub

  }

  @Override
  public void beHealthy() {
    // TODO Auto-generated method stub

  }

  @Override
  public String authenticate(InternalDistributedMember m, Object credentials)
      throws AuthenticationFailedException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Object getCredentials() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void emergencyClose() {
    // TODO Auto-generated method stub
    
  }

}
