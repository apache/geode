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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.PooledDistributionMessage;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.ServerLocator;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.logging.LogService;

/**
 * A processor for helping a locator find the durable queues for a given durable client id. Asks
 * each cache server if they have the durable id and builds a list of matching servers.
 *
 * @since GemFire 5.7
 */
public class FindDurableQueueProcessor extends ReplyProcessor21 {
  private static final Logger logger = LogService.getLogger();

  final ArrayList durableLocations = new ArrayList();

  public static ArrayList sendAndFind(ServerLocator locator, ClientProxyMembershipID proxyId,
      DistributionManager dm) {
    Set members = ((GridAdvisor) locator.getDistributionAdvisor()).adviseBridgeServers();
    if (members.contains(dm.getId())) {
      // Don't send message to local server, see #50534.
      Set remoteMembers = new HashSet(members);
      remoteMembers.remove(dm.getId());
      members = remoteMembers;
    }
    FindDurableQueueProcessor processor = new FindDurableQueueProcessor(dm, members);

    FindDurableQueueMessage.send(proxyId, dm, members, processor);
    try {
      processor.waitForRepliesUninterruptibly();
    } catch (ReplyException e) {
      e.handleCause();
    }
    ArrayList locations = processor.durableLocations;
    // This will add any local queues to the list
    findLocalDurableQueues(proxyId, locations);
    return locations;
  }

  private static void findLocalDurableQueues(ClientProxyMembershipID proxyId,
      ArrayList<ServerLocation> matches) {
    InternalCache cache = GemFireCacheImpl.getInstance();
    if (cache != null) {
      List l = cache.getCacheServers();
      if (l != null) {
        Iterator i = l.iterator();
        while (i.hasNext()) {
          CacheServerImpl bs = (CacheServerImpl) i.next();
          if (bs.getAcceptor().getCacheClientNotifier().getClientProxy(proxyId) != null) {
            ServerLocation loc = new ServerLocation(bs.getExternalAddress(), bs.getPort());
            matches.add(loc);
          }
        }
      }
    }
  }

  @Override
  public void process(DistributionMessage msg) {
    // TODO Auto-generated method stub
    if (msg instanceof FindDurableQueueReply) {
      FindDurableQueueReply reply = (FindDurableQueueReply) msg;
      synchronized (durableLocations) {
        // add me to the durable member set
        durableLocations.addAll(reply.getMatches());
      }
    }
    super.process(msg);
  }

  /**
   * Creates a new instance of FindDurableQueueProcessor
   */
  private FindDurableQueueProcessor(DistributionManager dm, Set members) {
    super(dm, members);
  }

  public static class FindDurableQueueMessage extends PooledDistributionMessage
      implements MessageWithReply {
    private int processorId;
    private ClientProxyMembershipID proxyId;

    protected static void send(ClientProxyMembershipID proxyId, DistributionManager dm, Set members,
        ReplyProcessor21 proc) {
      FindDurableQueueMessage msg = new FindDurableQueueMessage();
      msg.processorId = proc.getProcessorId();
      msg.proxyId = proxyId;
      msg.setRecipients(members);
      if (logger.isDebugEnabled()) {
        logger.debug("FindDurableQueueMessage sending {} to {}", msg, members);
      }
      dm.putOutgoing(msg);
    }

    @Override
    public int getProcessorId() {
      return this.processorId;
    }

    public ClientProxyMembershipID getProxyId() {
      return this.proxyId;
    }

    @Override
    protected void process(final ClusterDistributionManager dm) {
      ArrayList<ServerLocation> matches = new ArrayList<ServerLocation>();
      try {
        findLocalDurableQueues(proxyId, matches);

      } finally {
        FindDurableQueueReply reply = new FindDurableQueueReply();
        reply.setProcessorId(this.getProcessorId());
        reply.matches = matches;
        reply.setRecipient(getSender());
        if (dm.getId().equals(getSender())) {
          reply.setSender(getSender());
          reply.dmProcess(dm);
        } else {
          dm.putOutgoing(reply);
        }
      }
    }

    public int getDSFID() {
      return FIND_DURABLE_QUEUE;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
      // TODO Auto-generated method stub
      return super.clone();
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.processorId = in.readInt();
      this.proxyId = ClientProxyMembershipID.readCanonicalized(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeInt(this.processorId);
      DataSerializer.writeObject(this.proxyId, out);
    }

    @Override
    public String toString() {
      StringBuffer buff = new StringBuffer();
      buff.append("FindDurableQueueMessage (proxyId='").append(this.proxyId)
          .append("' processorId=").append(this.processorId).append(")");
      return buff.toString();
    }
  }

  public static class FindDurableQueueReply extends ReplyMessage {
    protected ArrayList matches = null;

    public ArrayList getMatches() {
      return this.matches;
    }

    @Override
    public int getDSFID() {
      return FIND_DURABLE_QUEUE_REPLY;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.matches = DataSerializer.readArrayList(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeArrayList(matches, out);
    }

    @Override
    public String toString() {
      StringBuffer buff = new StringBuffer();
      buff.append("FindDurableQueueReply (matches='").append(this.matches).append("' processorId=")
          .append(this.processorId).append(")");
      return buff.toString();
    }
  }
}
