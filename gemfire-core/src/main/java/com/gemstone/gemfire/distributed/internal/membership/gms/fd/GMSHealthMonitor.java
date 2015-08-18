package com.gemstone.gemfire.distributed.internal.membership.gms.fd;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.distributed.internal.membership.gms.Services;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.HealthMonitor;

/** Failure Detection */
public class GMSHealthMonitor implements HealthMonitor {

  private Services services;
  private NetView currentView;

  public static void loadEmergencyClasses() {
  }

  @Override
  public void contactedBy(InternalDistributedMember sender) {
    // TODO Auto-generated method stub
  }

  @Override
  public void suspect(InternalDistributedMember mbr, String reason) {
    // TODO Auto-generated method stub
  }

  @Override
  public boolean checkIfAvailable(DistributedMember mbr, String reason, boolean initiateRemoval) {
    // TODO Auto-generated method stub
    return true;
  }

  public void playDead(boolean b) {
    // TODO Auto-generated method stub
    
  }
  
  public void start() {
  }

  public void installView(NetView newView) {
    currentView = newView;
  }

  @Override
  public void init(Services s) {
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
  public void emergencyClose() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void memberShutdown(DistributedMember mbr, String reason) {
  }

}
