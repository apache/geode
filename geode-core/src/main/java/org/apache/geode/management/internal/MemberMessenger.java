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

package org.apache.geode.management.internal;

import java.util.Set;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.remote.AlertLevelChangeMessage;
import org.apache.geode.internal.logging.LogWriterImpl;

/**
 * This class will act as a messenger from manager to members for various
 * operations.
 * 
 * To start with its is designed to Manager start stop details, change alert
 * level.
 * 
 * 
 * 
 */
public class MemberMessenger {

  private MBeanJMXAdapter jmxAdapter;
  private ManagementResourceRepo repo;
  private InternalDistributedSystem system;

  public MemberMessenger(MBeanJMXAdapter jmxAdapter,
      ManagementResourceRepo repo, InternalDistributedSystem system) {
    this.jmxAdapter = jmxAdapter;
    this.repo = repo;
    this.system = system;

  }

  public void sendManagerInfo(DistributedMember receiver) {

    String levelName = jmxAdapter.getDistributedSystemMXBean().getAlertLevel();
    int alertCode = LogWriterImpl.levelNameToCode(levelName);
    ManagerStartupMessage msg = ManagerStartupMessage.create(alertCode);
    msg.setRecipient((InternalDistributedMember) receiver);
    sendAsync(msg);

  }

  public void broadcastManagerInfo() {
    Set<DistributedMember> otherMemberSet = system.getDistributionManager()
        .getAllOtherMembers();

    String levelName = jmxAdapter.getDistributedSystemMXBean().getAlertLevel();
    int alertCode = LogWriterImpl.levelNameToCode(levelName);
    ManagerStartupMessage msg = ManagerStartupMessage.create(alertCode);
    if (otherMemberSet != null && otherMemberSet.size() > 0) {
      msg.setRecipients(otherMemberSet);
    }

    sendAsync(msg);
    
    DM dm = system.getDistributionManager();
    if(dm instanceof DistributionManager){
      msg.process((DistributionManager)system.getDistributionManager());
    }
    

  }

  /**
   * Sends a message and does not wait for a response
   */
  void sendAsync(DistributionMessage msg) {
    if (system != null) {
      system.getDistributionManager().putOutgoing(msg);
    }
  }

  /**
   * Sets the alert level for this manager agent. Sends a
   * {@link AlertLevelChangeMessage} to each member of the distributed system.
   */
  public void setAlertLevel(String levelName) {
    int alertCode = LogWriterImpl.levelNameToCode(levelName);
    AlertLevelChangeMessage m = AlertLevelChangeMessage.create(alertCode);
    sendAsync(m);
  }
}
