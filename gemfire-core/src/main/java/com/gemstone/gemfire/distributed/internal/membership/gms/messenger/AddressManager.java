package com.gemstone.gemfire.distributed.internal.membership.gms.messenger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import org.apache.logging.log4j.Logger;
import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.protocols.PingData;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.UDP;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Responses;

import com.gemstone.gemfire.distributed.internal.membership.gms.Services;

/**
 * JGroups will not send messages that have IpAddress destination addresses.
 * Instead it requires a "logical address" and requests physical addresses
 * from the Discovery protocol that is normally in a JGroups stack.  We don't have
 * one of these, so we need to maintain a mapping between logical and physical
 * addresses.
 * 
 * @author bschuchardt
 *
 */
public class AddressManager extends Protocol {

  private static final Logger logger = Services.getLogger();
  
  private UDP udp;
  private Method setPingData;
  boolean warningLogged = false;

  @SuppressWarnings("unchecked")
  @Override
  public Object up(Event evt) {
    
//    logger.info("AddressManager.up: " + evt);
    
    switch (evt.getType()) {

    case Event.FIND_MBRS:
      List<Address> missing = (List<Address>)evt.getArg();
//      logger.debug("AddressManager.FIND_MBRS processing {}", missing);
      Responses responses = new Responses(false);
      for (Address laddr: missing) {
        try {
          if (laddr instanceof JGAddress) {
            PingData pd = new PingData(laddr, true, laddr.toString(), newIpAddress(laddr));
//            logger.debug("AddressManager.FIND_MBRS adding response {}", pd);
            responses.addResponse(pd, false);
            updateUDPCache(pd);
          }
        } catch (RuntimeException e) {
          logger.warn("Unable to create PingData response", e);
          throw e;
        }
      }
      return responses;
    }
    return up_prot.up(evt);
  }
  
  private IpAddress newIpAddress(Address jgaddr) {
    JGAddress addr = (JGAddress)jgaddr;
    return addr.asIpAddress();
  }
  
  /**
   * update the logical->physical address cache in UDP, which doesn't
   * seem to be updated by UDP when processing responses from FIND_MBRS
   * @param pd
   */
  private void updateUDPCache(PingData pd) {
    if (setPingData == null && !warningLogged) {
      findPingDataMethod();
    }
    if (setPingData != null) {
      Exception problem = null;
      try {
        setPingData.invoke(udp, new Object[]{pd});
      } catch (InvocationTargetException e) {
        problem = e;
      } catch (IllegalAccessException e) {
        problem = e;
      }
      if (problem != null && !warningLogged) {
        log.warn("Unable to update JGroups address cache - this may affect performance", problem);
        warningLogged = true;
      }
    }
  }
  
  /**
   * find and initialize the method used to update UDP's address cache
   */
  private void findPingDataMethod() {
    udp = (UDP)getProtocolStack().findProtocol("UDP");
    try {
      setPingData = TP.class.getDeclaredMethod("setPingData", new Class<?>[]{PingData.class});
      setPingData.setAccessible(true);
    } catch (NoSuchMethodException e) {
      if (!warningLogged) {
        log.warn("Unable to update JGroups address cache - this may affect performance", e);
        warningLogged = true;
      }
    }
  }
  
}
