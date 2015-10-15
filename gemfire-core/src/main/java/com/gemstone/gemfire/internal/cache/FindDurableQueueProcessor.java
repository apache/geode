/*=========================================================================
 * Copyright (c) 2003-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.MessageWithReply;
import com.gemstone.gemfire.distributed.internal.PooledDistributionMessage;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.distributed.internal.ServerLocator;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * A processor for helping a locator find the durable queues for a given durable client id.
 * Asks each bridge server if they have the durable id and builds a list of matching servers.
 *
 * @since 5.7
 * @author Greg Passmore
 */
public class FindDurableQueueProcessor extends ReplyProcessor21 {
  private static final Logger logger = LogService.getLogger();
  
  ////////// Public static entry point /////////

  final ArrayList durableLocations = new ArrayList();

  // @todo gregp: add javadocs
  public static ArrayList sendAndFind(ServerLocator locator, ClientProxyMembershipID proxyId,
                   DM dm) {
    Set members = ((ControllerAdvisor)locator.getDistributionAdvisor()).adviseBridgeServers();
    if (members.contains(dm.getId())) {
      // Don't send message to local server, see #50534.
      Set remoteMembers = new HashSet(members);
      remoteMembers.remove(dm.getId());
      members = remoteMembers;
    }
    FindDurableQueueProcessor processor = 
      new FindDurableQueueProcessor(dm,members);
      
    FindDurableQueueMessage.send(proxyId, dm, members,processor);
    try {
      processor.waitForRepliesUninterruptibly();
    } catch (ReplyException e) {
      e.handleAsUnexpected();
    }
    ArrayList locations = processor.durableLocations;
    //This will add any local queues to the list
    findLocalDurableQueues(proxyId, locations);
    return locations;
  }
  
  private static void findLocalDurableQueues(ClientProxyMembershipID proxyId, ArrayList<ServerLocation> matches) {
    Cache c = GemFireCacheImpl.getInstance();
    if(c!=null) {
      List l = c.getCacheServers();
      if(l!=null) {
        Iterator i = l.iterator();
        while(i.hasNext()) {
          CacheServerImpl bs = (CacheServerImpl)i.next();
          if(bs.getAcceptor().getCacheClientNotifier().getClientProxy(proxyId)!=null) {
            ServerLocation loc = new ServerLocation(bs.getExternalAddress(),bs.getPort());
            matches.add(loc);
          }
        }
      }
    }
  }
  

  ////////////  Instance methods //////////////
  
  @Override
  public void process(DistributionMessage msg) {
    // TODO Auto-generated method stub
    if(msg instanceof FindDurableQueueReply) {
      FindDurableQueueReply reply = (FindDurableQueueReply)msg;
        synchronized(durableLocations) {
          //add me to the durable member set
          durableLocations.addAll(reply.getMatches());
        }
    }
    super.process(msg);
  }


  /** Creates a new instance of FindDurableQueueProcessor
   */
  private FindDurableQueueProcessor(DM dm,Set members) {
    super(dm, members);
  }

  
  ///////////////   Inner message classes  //////////////////
  
  public static class FindDurableQueueMessage
    extends PooledDistributionMessage implements MessageWithReply
  {
    private int processorId;
    private ClientProxyMembershipID proxyId;
    
    protected static void send(ClientProxyMembershipID proxyId,
                             DM dm,Set members, 
                             ReplyProcessor21 proc)
    {
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
    protected void process(final DistributionManager dm) {
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
        }
        else {
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
    public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
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
      buff.append("FindDurableQueueMessage (proxyId='")
        .append(this.proxyId)
        .append("' processorId=")
        .append(this.processorId)
        .append(")");
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
    public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.matches =  DataSerializer.readArrayList(in);
    }
  
    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeArrayList(matches, out);
    }
  
    @Override
    public String toString() {
      StringBuffer buff = new StringBuffer();
      buff.append("FindDurableQueueReply (matches='")
        .append(this.matches)
        .append("' processorId=")
        .append(this.processorId)
        .append(")");
      return buff.toString();
    }
  }
}

