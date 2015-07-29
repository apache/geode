package com.gemstone.gemfire.distributed.internal.membership.gms.interfaces;

import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.distributed.internal.membership.gms.GMSMemberServices;

/**
 * Services in GMS all implement this interface
 *
 */
public interface Service {
  void init(GMSMemberServices s);

  /**
   * called after all services have been initialized 
   * with init() and all services are available via Services
   */
  void start();

  /**
   * called after all servers have been started
   */
  void started();

  /**
   * called when the GMS is stopping
   */
  void stop();

  /**
   * called after all services have been stopped
   */
  void stopped();

  /**
   * called when a new view is installed by Membership
   */
  void installView(NetView v);

  /**
   * test method for simulating a sick/dead member
   */
  void beSick();
  
  /**
   * test method for simulating a sick/dead member
   */
  void playDead();
  
  /**
   * test method for simulating a sick/dead member
   */
  void beHealthy();
  
  /**
   * shut down threads, cancel timers, etc.
   */
  void emergencyClose();
}
