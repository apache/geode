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
package org.apache.geode.internal.cache.tier.sockets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.PooledDistributionMessage;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.logging.LogService;

/**
 * Distribution message for dropping client from denylist.
 *
 * @since GemFire 6.0
 *
 */
public class RemoveClientFromDenylistMessage extends PooledDistributionMessage {
  private static final Logger logger = LogService.getLogger();

  // The proxy id of the client represented by this proxy
  private ClientProxyMembershipID proxyID;

  @Override
  protected void process(ClusterDistributionManager dm) {
    Cache c = dm.getCache();
    if (c != null) {
      List l = c.getCacheServers();
      if (l != null) {
        Iterator i = l.iterator();
        while (i.hasNext()) {
          CacheServerImpl bs = (CacheServerImpl) i.next();
          CacheClientNotifier ccn = bs.getAcceptor().getCacheClientNotifier();
          Set s = ccn.getDenylistedClient();
          if (s != null) {
            if (s.remove(proxyID)) {
              DistributedSystem ds = dm.getSystem();
              if (ds != null) {
                if (logger.isDebugEnabled()) {
                  logger.debug(
                      "Remove the client from deny list as its queue is already destroyed: {}",
                      proxyID);
                }
              }
            }
          }
        }
      }
    }
  }

  public RemoveClientFromDenylistMessage() {
    this.setRecipient(ALL_RECIPIENTS);
  }

  public void setProxyID(ClientProxyMembershipID proxyID) {
    this.proxyID = proxyID;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(proxyID, out);
  }

  public int getDSFID() {
    return REMOVE_CLIENT_FROM_DENYLIST_MESSAGE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    proxyID = ClientProxyMembershipID.readCanonicalized(in);
  }

}
