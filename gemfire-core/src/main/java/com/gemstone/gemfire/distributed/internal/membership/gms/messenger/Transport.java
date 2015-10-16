package com.gemstone.gemfire.distributed.internal.membership.gms.messenger;

import java.lang.reflect.InvocationTargetException;

import org.jgroups.protocols.UDP;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.LazyThreadFactory;

public class Transport extends UDP {
  
  /*
   * (non-Javadoc)
   * Copied from JGroups 3.6.6 UDP and modified to suppress
   * stack traces when unable to set the ttl due to the TCP implementation
   * not supporting the setting, and to only set ttl when multicast is
   * going to be used.
   * 
   * @see org.jgroups.protocols.UDP#setTimeToLive(int)
   */
  @Override
  protected void setTimeToLive(int ttl) {
    if (ip_mcast) {
      if(getImpl != null && setTimeToLive != null) {
        try {
            Object impl=getImpl.invoke(sock);
            setTimeToLive.invoke(impl, ttl);
        }
        catch(InvocationTargetException e) {
          log.info("Unable to set ip_ttl - TCP/IP implementation does not support this setting");
        }
        catch(Exception e) {
            log.error("failed setting ip_ttl", e);
        }
      } else {
        log.warn("ip_ttl %d could not be set in the datagram socket; ttl will default to 1 (getImpl=%s, " +
                   "setTimeToLive=%s)", ttl, getImpl, setTimeToLive);
      }
    }
  }

    
  /*
   * (non-Javadoc)
   * JGroups does not currently (3.6.6) allow you to specify that
   * threads should be daemon, so we override the init() method here
   * and create the factories before initializing UDP
   * @see org.jgroups.protocols.UDP#init()
   */
  @Override
  public void init() throws Exception {
    global_thread_factory=new DefaultThreadFactory("Geode ", true);
    timer_thread_factory=new LazyThreadFactory("Geode UDP Timer", true, true);
    default_thread_factory=new DefaultThreadFactory("Geode UDP Incoming", true, true);
    oob_thread_factory=new DefaultThreadFactory("Geode UDP OOB", true, true);
    internal_thread_factory=new DefaultThreadFactory("Geode UDP INT", true, true);
    super.init();
  }

}
