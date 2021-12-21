/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.distributed.internal.membership.gms.messenger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import org.apache.logging.log4j.Logger;
import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.protocols.PingData;
import org.jgroups.protocols.TP;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Responses;

import org.apache.geode.distributed.internal.membership.gms.Services;

/**
 * JGroups will not send messages that have IpAddress destination addresses. Instead it requires a
 * "logical address" and requests physical addresses from the Discovery protocol that is normally in
 * a JGroups stack. We don't have one of these, so we need to maintain a mapping between logical and
 * physical addresses.
 */
public class AddressManager extends Protocol {

  private static final Logger logger = Services.getLogger();

  private TP transport;
  private Method setPingData;
  boolean warningLogged = false;

  @SuppressWarnings("unchecked")
  @Override
  public Object up(Event evt) {

    switch (evt.getType()) {

      case Event.FIND_MBRS:
        List<Address> missing = (List<Address>) evt.getArg();

        Responses responses = new Responses(false);
        for (Address laddr : missing) {
          try {
            if (laddr instanceof JGAddress) {
              PingData pd = new PingData(laddr, true, laddr.toString(), newIpAddress(laddr));
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
    JGAddress addr = (JGAddress) jgaddr;
    return addr.asIpAddress();
  }

  /**
   * update the logical->physical address cache in UDP, which doesn't seem to be updated by UDP when
   * processing responses from FIND_MBRS
   *
   */
  private void updateUDPCache(PingData pd) {
    if (setPingData == null && !warningLogged) {
      findPingDataMethod();
    }
    if (setPingData != null) {
      try {
        setPingData.invoke(transport, pd);
      } catch (InvocationTargetException | IllegalAccessException e) {
        if (!warningLogged) {
          log.warn("Unable to update JGroups address cache - this may affect performance", e);
          warningLogged = true;
        }
      }
    }
  }

  /**
   * find and initialize the method used to update UDP's address cache
   */
  private void findPingDataMethod() {
    transport = getProtocolStack().getTransport();
    try {
      setPingData = TP.class.getDeclaredMethod("setPingData", PingData.class);
      setPingData.setAccessible(true);
    } catch (NoSuchMethodException e) {
      if (!warningLogged) {
        log.warn("Unable to update JGroups address cache - this may affect performance", e);
        warningLogged = true;
      }
    }
  }

}
