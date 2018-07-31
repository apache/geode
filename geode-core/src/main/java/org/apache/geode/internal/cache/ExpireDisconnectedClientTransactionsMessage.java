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
package org.apache.geode.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.HighPriorityDistributionMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Version;

public class ExpireDisconnectedClientTransactionsMessage
    extends HighPriorityDistributionMessage {
  Set<TXId> txIds;

  /** for deserialization */
  public ExpireDisconnectedClientTransactionsMessage() {}

  // only send to geode 1.7.0 and later servers
  // for prior geode 1.7.0 servers, message won't be sent
  // assuming these servers will be rolled to new version soon.
  static void send(DistributionManager dm, Set<InternalDistributedMember> recipients,
      Set<TXId> txIds) {
    ExpireDisconnectedClientTransactionsMessage msg =
        new ExpireDisconnectedClientTransactionsMessage();
    msg.txIds = txIds;
    Set newVersionRecipients = new HashSet();
    for (InternalDistributedMember recipient : recipients) {
      // to geode 1.7.0 and later version servers
      if (recipient.getVersionObject().compareTo(Version.GEODE_170) >= 0) {
        newVersionRecipients.add(recipient);
      }
    }
    msg.setRecipients(newVersionRecipients);
    dm.putOutgoing(msg);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeHashSet((HashSet<TXId>) this.txIds, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.txIds = DataSerializer.readHashSet(in);
  }

  public int getDSFID() {
    return EXPIRE_CLIENT_TRANSACTIONS;
  }

  @Override
  protected void process(ClusterDistributionManager dm) {
    InternalCache cache = dm.getCache();
    InternalDistributedMember sender = getSender();
    if (cache != null) {
      TXManagerImpl mgr = cache.getTXMgr();
      if (sender.getVersionObject().compareTo(Version.GEODE_170) >= 0) {
        // schedule to expire disconnected client transaction.
        mgr.expireDisconnectedClientTransactions(this.txIds, false);
      } else {
        // check if transaction has been updated before remove it
        mgr.removeExpiredClientTransactions(this.txIds);
      }
    }
  }
}
