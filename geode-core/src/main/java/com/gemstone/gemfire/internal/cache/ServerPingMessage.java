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
package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.PooledDistributionMessage;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.Version;

/**
 * Ping to check if a server is alive. It waits for a specified 
 * time before returning false. 
 * 
 */
public class ServerPingMessage extends PooledDistributionMessage {
  private int processorId = 0;
  
  public ServerPingMessage() {
  }

  
  public ServerPingMessage(ReplyProcessor21 processor) {
    this.processorId = processor.getProcessorId();
  }

  
  @Override
  public int getDSFID() {
    return SERVER_PING_MESSAGE;
  }

  /**
   * Sends a ping message. The pre-GFXD_101 recipients are filtered out 
   * and it is assumed that they are pingable. 
   * 
   * @return true if all the recipients are pingable
   */
  public static boolean send (GemFireCacheImpl cache, 
      Set<InternalDistributedMember> recipients) {
    
    InternalDistributedSystem ids = cache.getDistributedSystem();
    DM dm = ids.getDistributionManager();
    Set <InternalDistributedMember> filteredRecipients = new HashSet<InternalDistributedMember>();
     
    // filtered recipients 
    for (InternalDistributedMember recipient : recipients) {
      if(Version.GFE_81.compareTo(recipient.getVersionObject()) <= 0) {
        filteredRecipients.add(recipient);
      } 
    }
    if (filteredRecipients == null || filteredRecipients.size() == 0)
      return true;
   
    ReplyProcessor21 replyProcessor = new ReplyProcessor21(dm, filteredRecipients);
    ServerPingMessage spm = new ServerPingMessage(replyProcessor);
   
    spm.setRecipients(filteredRecipients);
    Set failedServers = null;
    try {
      if (cache.getLoggerI18n().fineEnabled())
        cache.getLoggerI18n().fine("Pinging following servers " +  filteredRecipients);
      failedServers = dm.putOutgoing(spm);
      
      // wait for the replies for timeout msecs
      boolean receivedReplies = replyProcessor.waitForReplies(0L);
      
      dm.getCancelCriterion().checkCancelInProgress(null);
      
      // If the reply is not received in the stipulated time, throw an exception
      if (!receivedReplies) {
        cache.getLoggerI18n().error(LocalizedStrings.Server_Ping_Failure, filteredRecipients);
        return false;
      }
    } catch (Throwable e) {
      cache.getLoggerI18n().error(LocalizedStrings.Server_Ping_Failure, filteredRecipients, e );
      return false;
    }
   
    if (failedServers == null  || failedServers.size() == 0)
      return true; 
    
    cache.getLoggerI18n().info(LocalizedStrings.Server_Ping_Failure, failedServers);
    
    return false;
  }
  
  @Override
  protected void process(DistributionManager dm) {
    // do nothing. We are just pinging the server. send the reply back. 
    ReplyMessage.send(getSender(), this.processorId,  null, dm); 
  }

 
  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.processorId);
  }
  
  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.processorId = in.readInt();
  }
  
  @Override
  public int getProcessorId() {
    return this.processorId;
  }
}
