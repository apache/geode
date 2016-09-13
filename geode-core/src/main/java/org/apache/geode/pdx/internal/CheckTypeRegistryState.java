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
package org.apache.geode.pdx.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.HighPriorityDistributionMessage;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.pdx.PdxInitializationException;

public class CheckTypeRegistryState extends HighPriorityDistributionMessage 
          implements MessageWithReply {
  private int processorId;

  public CheckTypeRegistryState() {
    super();
  }

  protected CheckTypeRegistryState(int processorId) {
    super();
    this.processorId = processorId;
  }

  public static void send(DM dm) {
    Set recipients = dm.getOtherDistributionManagerIds();
    ReplyProcessor21 replyProcessor = new ReplyProcessor21(dm, recipients);
    CheckTypeRegistryState msg = new CheckTypeRegistryState(replyProcessor.getProcessorId());
    msg.setRecipients(recipients);
    dm.putOutgoing(msg);
    try {
      replyProcessor.waitForReplies();
    } catch (ReplyException e) {
      if(e.getCause() instanceof PdxInitializationException) {
        throw new PdxInitializationException("Bad PDX configuration on member "
            + e.getSender() + ": " + e.getCause().getMessage(), e.getCause());
      }
      else {
        throw new InternalGemFireError("Unexpected exception", e);
      }
    } catch (InterruptedException e) {
      throw new InternalGemFireError("Unexpected exception", e);
    }
  }

  @Override
  protected void process(DistributionManager dm) {
    ReplyException e = null;
    try {
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      if(cache != null && !cache.isClosed()) {
        TypeRegistry pdxRegistry = cache.getPdxRegistry();
        if(pdxRegistry != null) {
          TypeRegistration registry = pdxRegistry.getTypeRegistration();
          if(registry instanceof PeerTypeRegistration) {
            PeerTypeRegistration peerRegistry = (PeerTypeRegistration) registry;
            peerRegistry.verifyConfiguration();
          }
        }
      }
    } catch(Exception ex) {
      e = new ReplyException(ex);
    } finally {
      ReplyMessage rm = new ReplyMessage();
      rm.setException(e);
      rm.setProcessorId(processorId);
      rm.setRecipient(getSender());
      dm.putOutgoing(rm);
    }
  }

  public int getDSFID() {
    return CHECK_TYPE_REGISTRY_STATE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.processorId = in.readInt();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.processorId);
  }
}
