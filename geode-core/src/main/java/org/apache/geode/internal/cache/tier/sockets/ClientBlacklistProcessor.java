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
package org.apache.geode.internal.cache.tier.sockets;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.PooledDistributionMessage;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
/**
 * A processor for sending client black list message to all nodes from primary.
 * This adds client to the blacklist and destroy it's queue if available on node.
 * 
 * @since GemFire 6.0
 *
 */
public class ClientBlacklistProcessor extends ReplyProcessor21 {
  
   public static void sendBlacklistedClient(ClientProxyMembershipID proxyId,
      DM dm, Set members) {
    ClientBlacklistProcessor processor = new ClientBlacklistProcessor(dm,
        members);
    ClientBlacklistMessage.send(proxyId, dm, processor, members);
    try {
      processor.waitForRepliesUninterruptibly();
    }
    catch (ReplyException e) {
      e.handleAsUnexpected();
    }
    return;
  }
 
  ////////////// Instance methods //////////////
  
  @Override
  public void process(DistributionMessage msg) {
      super.process(msg);
  }
  /** Creates a new instance of ClientBlacklistProcessor
   */
  private ClientBlacklistProcessor(DM dm, Set members) {
    super(dm, members);
  }
  
  ///////////////   Inner message classes  //////////////////
  
  public static class ClientBlacklistMessage extends PooledDistributionMessage
      implements MessageWithReply {
    private int processorId;

    private ClientProxyMembershipID proxyId;

    protected static void send(ClientProxyMembershipID proxyId, DM dm,
        ClientBlacklistProcessor proc, Set members) {
      ClientBlacklistMessage msg = new ClientBlacklistMessage();
      msg.processorId = proc.getProcessorId();
      msg.proxyId = proxyId;
      msg.setRecipients(members);
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
    protected void process(final DistributionManager dm) {
      try {
        Cache c = GemFireCacheImpl.getInstance();
        if (c != null) {
          List l = c.getCacheServers();
          if (l != null) {
            Iterator i = l.iterator();
            while (i.hasNext()) {
              CacheServerImpl bs = (CacheServerImpl)i.next();
              CacheClientNotifier ccn = bs.getAcceptor().getCacheClientNotifier(); 
              //add client to the black list.
              ccn.addToBlacklistedClient(this.proxyId);
              CacheClientProxy proxy = ccn.getClientProxy(this.proxyId); 
              if(proxy != null) {
              //close the proxy and remove from client proxy list.
                proxy.close(false,false);
                ccn.removeClientProxy(proxy);
            }
          }
        }
       }   
      }
      finally {
        ClientBlacklistReply reply = new ClientBlacklistReply();
        reply.setProcessorId(this.getProcessorId());
        reply.setRecipient(getSender());
        if (dm.getId().equals(getSender())) {
          reply.setSender(getSender());
          reply.dmProcess(dm);
        }
        else {
          dm.putOutgoing(reply);
        }
      }
    }

    public int getDSFID() {
      return CLIENT_BLACKLIST_MESSAGE;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
      return super.clone();
    }

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
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
      buff.append("ClientBlacklistMessage (proxyId='").append(this.proxyId)
          .append("' processorId=").append(this.processorId).append(")");
      return buff.toString();
    }
  }
  
  public static class ClientBlacklistReply extends ReplyMessage {
  }

}

