/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.distributed.internal;

import java.io.NotSerializableException;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.MembershipManager;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.logging.log4j.LogMarker;

/**
 * To change this generated comment edit the template variable "typecomment":
 * Window>Preferences>Java>Templates.
 * To enable and disable the creation of type comments go to
 * Window>Preferences>Java>Code Generation.
 */
public class DistributionChannel  {
  private static final Logger logger = LogService.getLogger();
  
  private MembershipManager membershipManager;
  
  /**
   * Constructor DistributionChannel for JGroups.
   * @param channel jgroups channel
   */
  public DistributionChannel(MembershipManager channel) {
    membershipManager = channel;
  }


  public InternalDistributedMember getLocalAddress() {
    return membershipManager.getLocalMember();
  }


  /**
   * @return the MembershipManager
   */
  public MembershipManager getMembershipManager() {
    return membershipManager;
  }



  /**
   * @return list of recipients who did not receive the message because
   * they left the view (null if all received it or it was sent to
   * {@link DistributionMessage#ALL_RECIPIENTS}).
   * @throws NotSerializableException
   *         If content cannot be serialized
   */
  public Set send(InternalDistributedMember[] destinations,
                  DistributionMessage content,
                  DistributionManager dm, DistributionStats stats)
  throws NotSerializableException {
    if (membershipManager == null) {
      logger.warn(LocalizedMessage.create(LocalizedStrings.DistributionChannel_ATTEMPTING_A_SEND_TO_A_DISCONNECTED_DISTRIBUTIONMANAGER));
      if (destinations.length == 1 
          && destinations[0] == DistributionMessage.ALL_RECIPIENTS)
        return null;
      HashSet result = new HashSet();
      for (int i = 0; i < destinations.length; i ++)
        result.add(destinations[i]);
      return result;
      }
    return membershipManager.send(destinations, content, stats);
  }

  public void disconnect(boolean duringStartup)
  {
    StringBuffer sb = new StringBuffer();
    sb.append("Disconnected from distribution channel ");

    long start = System.currentTimeMillis();

    logger.debug("DistributionChannel disconnecting with "+ membershipManager + "; duringStartup="+duringStartup);
    
    if (membershipManager != null) {
      sb.append(membershipManager.getLocalMember());
      sb.append(" (took ");
      long begin = System.currentTimeMillis();
      if (duringStartup) {
        membershipManager.uncleanShutdown("Failed to start distribution", null);
      }
      else {
        membershipManager.shutdown();
      }
      long delta = System.currentTimeMillis() - begin;
      sb.append(delta);
      sb.append("/");
    }
    membershipManager = null;

    if (logger.isTraceEnabled(LogMarker.DM)) {
      long delta = System.currentTimeMillis() - start;
      sb.append(delta);
      sb.append(" ms)");
      logger.trace(LogMarker.DM, sb);
    }
  }

  /**
   * Returns the id of this distribution channel.  If this channel
   * uses JavaGroups and the conduit to communicate with others, then
   * the port of the JavaGroups channel's {@link InternalDistributedMember address} is
   * returned.
   *
   * @since GemFire 3.0
   */
  public long getId() {
    MembershipManager mgr = this.membershipManager;
    if (mgr == null) {
      throw new DistributedSystemDisconnectedException(LocalizedStrings.DistributionChannel_I_NO_LONGER_HAVE_A_MEMBERSHIP_ID.toLocalizedString());
    }
    InternalDistributedMember moi = mgr.getLocalMember();
    if (moi == null) {
      throw new DistributedSystemDisconnectedException(LocalizedStrings.DistributionChannel_I_NO_LONGER_HAVE_A_MEMBERSHIP_ID.toLocalizedString(), membershipManager.getShutdownCause());
    }
    return moi.getPort();
  }

  public void setShutDown() {
//    this.shuttingDown = shuttingDown;
    if (membershipManager != null)
      membershipManager.setShutdown();
  }

//   private void sendViaJGroups(Serializable[] destinations,Address source,Serializable content,
//                          boolean deliverToSender, int processorType,
//                          DistributionManager dm)
//   throws ChannelNotConnectedException, ChannelClosedException {
//     Message msg = new Message(null, source, content);
//     msg.setDeliverToSender(deliverToSender);
//     msg.setProcessorType(processorType);
//     for (int i=0; i < destinations.length; i++) {
//       Address destination = (Address) destinations[i];
//       msg.setDest(destination);
//       jgroupsChannel.send(msg);
//       if (destination == null)
//         break;
//     }
//   }

}
