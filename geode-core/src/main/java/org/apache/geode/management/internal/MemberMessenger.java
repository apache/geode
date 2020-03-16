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
package org.apache.geode.management.internal;

import java.util.Set;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.remote.AlertLevelChangeMessage;
import org.apache.geode.logging.internal.log4j.LogLevel;

/**
 * Handles messaging from manager to members for various operations. It sends two types of messages:
 * {@link ManagerStartupMessage} and {@link AlertLevelChangeMessage}
 */
public class MemberMessenger {

  private final MBeanJMXAdapter jmxAdapter;
  private final InternalDistributedSystem system;

  MemberMessenger(MBeanJMXAdapter jmxAdapter, InternalDistributedSystem system) {
    this.jmxAdapter = jmxAdapter;
    this.system = system;
  }

  /**
   * <pre>
   * Code path 1:  {@link FederatingManager#startManager()} ->
   *               FederatingManager.startManagingActivity() ->
   *               FederatingManager.GIITask.call() ->
   *               {@code sendManagerInfo}
   * Recipient(s): {@link DistributionManager#getOtherDistributionManagerIds()}
   * When:         starting the manager
   * </pre>
   *
   * <p>
   *
   * <pre>
   * Code path 2:  FederatingManager.addMember(DistributedMember)} ->
   *               FederatingManager.GIITask#call() ->
   *               {@code sendManagerInfo}
   * Recipient(s): one new member from membership listener
   * When:         new member joins
   * </pre>
   *
   * <p>
   * This path does <b>NOT</b> process {@link ManagerStartupMessage} locally so we do not add an
   * AlertListener for local Alerts.
   */
  void sendManagerInfo(DistributedMember receiver) {
    String levelName = jmxAdapter.getDistributedSystemMXBean().getAlertLevel();
    int alertCode = LogLevel.getLogWriterLevel(levelName);

    ManagerStartupMessage msg = ManagerStartupMessage.create(alertCode);
    msg.setRecipient((InternalDistributedMember) receiver);

    sendAsync(msg);
  }

  /**
   * <pre>
   * Code path:    {@link FederatingManager#startManager()} ->
   *               {@code sendManagerInfo}
   * Recipient(s): {@link DistributionManager#getAllOtherMembers()}
   * When:         starting the manager
   * </pre>
   *
   * <p>
   * This call seems to be redundant with {@link #sendManagerInfo(DistributedMember)} except that
   * here we do not switch context to another thread and we also register a local AlertListener.
   *
   * <p>
   * After sending to remote members, this call processes the {@link ManagerStartupMessage} locally
   * which would add an AlertListener for local Alerts. But local Alerts don't seem to work
   * (GEODE-5923).
   */
  void broadcastManagerInfo() {
    Set<InternalDistributedMember> otherMemberSet =
        system.getDistributionManager().getAllOtherMembers();

    String levelName = jmxAdapter.getDistributedSystemMXBean().getAlertLevel();
    int alertCode = LogLevel.getLogWriterLevel(levelName);

    ManagerStartupMessage msg = ManagerStartupMessage.create(alertCode);
    if (otherMemberSet != null && otherMemberSet.size() > 0) {
      msg.setRecipients(otherMemberSet);
    }

    sendAsync(msg);

    DistributionManager dm = system.getDistributionManager();
    if (dm instanceof ClusterDistributionManager) {
      msg.process((ClusterDistributionManager) system.getDistributionManager());
    }
  }

  /**
   * Sends a message and does not wait for a response.
   *
   * <p>
   * Actually, it's the message implementation that determines if it's sync or async, not this call.
   */
  private void sendAsync(DistributionMessage msg) {
    if (system != null) {
      system.getDistributionManager().putOutgoing(msg);
    }
  }

  /**
   * Sets the alert level for this manager agent. Sends a {@link AlertLevelChangeMessage} to each
   * member of the distributed system.
   */
  public void setAlertLevel(String levelName) {
    int alertCode = LogLevel.getLogWriterLevel(levelName);
    AlertLevelChangeMessage m = AlertLevelChangeMessage.create(alertCode);
    sendAsync(m);
  }
}
