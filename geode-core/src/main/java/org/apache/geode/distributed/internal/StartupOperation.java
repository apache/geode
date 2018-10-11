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
package org.apache.geode.distributed.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.remote.RemoteTransportConfig;
import org.apache.geode.internal.logging.LogService;

public class StartupOperation {
  private static final Logger logger = LogService.getLogger();

  ClusterDistributionManager dm;
  RemoteTransportConfig transport;
  Set newlyDeparted;

  StartupOperation(ClusterDistributionManager dm, RemoteTransportConfig transport) {
    this.dm = dm;
    this.transport = transport;
  }

  /**
   * send the startup message and wait for responses. If timeout is zero, this waits forever for all
   * responses to come back. This method may throw jgroups and other io exceptions since it
   * interacts with the distribution manager at a low level to send the startup messages. It does
   * this to ensure it knows which recipients didn't receive the message.
   *
   * @return whether all recipients could be contacted. The failure set can be fetched with
   *         getFailureSet??
   */
  boolean sendStartupMessage(Set recipients, long timeout, Set<InetAddress> interfaces,
      String redundancyZone,
      boolean enforceUniqueZone)
      throws InterruptedException, ReplyException, java.net.UnknownHostException, IOException {
    if (Thread.interrupted())
      throw new InterruptedException();
    StartupMessageReplyProcessor proc = new StartupMessageReplyProcessor(dm, recipients);
    boolean isSharedConfigurationEnabled = false;
    if (InternalLocator.hasLocator()) {
      isSharedConfigurationEnabled = InternalLocator.getLocator().isSharedConfigurationEnabled();
    }
    StartupMessage msg =
        new StartupMessage(InternalLocator.getLocatorStrings(), isSharedConfigurationEnabled);

    msg.setInterfaces(interfaces);
    msg.setDistributedSystemId(dm.getConfig().getDistributedSystemId());
    msg.setRedundancyZone(redundancyZone);
    msg.setEnforceUniqueZone(enforceUniqueZone);
    msg.setMcastEnabled(transport.isMcastEnabled());
    msg.setMcastPort(dm.getSystem().getOriginalConfig().getMcastPort());
    msg.setMcastHostAddress(dm.getSystem().getOriginalConfig().getMcastAddress());
    msg.setTcpDisabled(transport.isTcpDisabled());
    msg.setRecipients(recipients);
    msg.setReplyProcessorId(proc.getProcessorId());

    this.newlyDeparted = dm.sendOutgoing(msg); // set of departed jgroups ids
    if (this.newlyDeparted != null && !this.newlyDeparted.isEmpty()) {
      // tell the reply processor not to wait for the recipients that didn't
      // get the message
      // Vector viewMembers = dm.getViewMembers();
      for (Iterator it = this.newlyDeparted.iterator(); it.hasNext();) {
        InternalDistributedMember id = (InternalDistributedMember) it.next();
        this.dm.handleManagerDeparture(id, false,
            "left the membership view");
        proc.memberDeparted(this.dm, id, true);
      }
    }

    if (proc.stillWaiting() && logger.isDebugEnabled()) {
      logger.debug("Waiting {} milliseconds to receive startup responses", timeout);
    }
    boolean timedOut = true;
    Set<InternalDistributedMember> unresponsive = null;
    try {
      timedOut = !proc.waitForReplies(timeout);
    } finally {
      if (timedOut) {
        unresponsive = new HashSet<>();
        proc.collectUnresponsiveMembers(unresponsive);
        if (!unresponsive.isEmpty()) {
          for (Iterator it = unresponsive.iterator(); it.hasNext();) {
            InternalDistributedMember um = (InternalDistributedMember) it.next();
            if (!dm.getViewMembers().contains(um)) {
              // Member slipped away and we didn't notice.
              it.remove();
              dm.handleManagerDeparture(um, true,
                  "disappeared during startup handshake");
            } else if (dm.isCurrentMember(um)) {
              // the member must have connected back to us and now we just
              // need to get its startup response
              logger.warn(
                  "Membership: received connection from <{}> but received no startup response after {} ms.",
                  new Object[] {um, timeout});
            }
          } // for

          // Tell the dm who we expect to be waiting for...
          this.dm.setUnfinishedStartups(unresponsive);

          // Re-examine list now that we have elided the startup problems....
          if (!unresponsive.isEmpty()) {
            logger.warn(
                "Membership: startup timed out after waiting {} milliseconds for responses from {}",
                new Object[] {Long.valueOf(timeout), unresponsive});
          }
        } // !isEmpty
      } // timedOut
    } // finally

    boolean problems;
    problems = this.newlyDeparted != null && this.newlyDeparted.size() > 0;
    // problems = problems ||
    // (unresponsive != null && unresponsive.size() > 0);
    return !problems;
  }
}
