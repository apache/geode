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

package org.apache.geode.internal.cache;

import java.util.*;
import java.io.*;
import org.apache.geode.*;
import org.apache.geode.cache.*;
import org.apache.geode.distributed.*;
import org.apache.geode.distributed.internal.*;


/**
 * Sends a chunk of queued messages to everyone currently playing a role.
 *
 * @since GemFire 5.0
 *
 */
public class SendQueueOperation {
  //private ReplyProcessor21 processor = null;
  private DM dm;
  private DistributedRegion r;
  private List l;
  private Role role;

  SendQueueOperation(DM dm, DistributedRegion r, List l, Role role) {
    this.dm = dm;
    this.r = r;
    this.l = l;
    this.role = role;
  }

  /**
   * Returns true if distribution successful. Also modifies message list by
   * removing messages sent to the required role.
   */
  boolean distribute() {
    CacheDistributionAdvisor advisor = this.r.getCacheDistributionAdvisor();
    Set recipients = advisor.adviseCacheOpRole(this.role);
    if (recipients.isEmpty()) {
      return false;
    }
    ReplyProcessor21 processor = new ReplyProcessor21(this.dm, recipients);
    // @todo darrel: make this a reliable one
    SendQueueMessage msg = new SendQueueMessage();
    msg.setRecipients(recipients);
    msg.setRegionPath(this.r.getFullPath());
    msg.setProcessorId(processor.getProcessorId());
    msg.setOperations(this.l);
    dm.putOutgoing(msg);
    try {
      processor.waitForReplies();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      // It's OK to keep going, no significant work below.
    } catch (ReplyException ex) {
      ex.handleAsUnexpected();
    }
    if (msg.getSuccessfulRecipients().isEmpty()) {
      return false;
    }
    // @todo darrel: now remove sent items from the list
    this.r.getCachePerfStats().incReliableQueuedOps(- l.size());
    this.l.clear();
    return true;
  }

  /**
   * A batch of queued messages. Once they are processed on the other side
   * an ack is sent.
   */
  public static final class SendQueueMessage
    extends SerialDistributionMessage
    implements MessageWithReply
  {
    private int processorId;
    private String regionPath;
    /**
     * List of QueuedOperation instances
     */
    private List ops;

    @Override
    public int getProcessorId() {
      return this.processorId;
    }
    public void setProcessorId(int id) {
      this.processorId = id;
    }
    public String getRegionPath() {
      return this.regionPath;
    }
    public void setRegionPath(String rp) {
      this.regionPath = rp;
    }
    public void setOperations(List l) {
      this.ops = l;
    }
    @Override
    protected void process(DistributionManager dm) {
      ReplyException rex = null;
      boolean ignored = false;
      try {
        GemFireCacheImpl gfc = (GemFireCacheImpl)CacheFactory.getInstance(dm.getSystem());
        final LocalRegion lclRgn = gfc.getRegionByPathForProcessing(this.regionPath);
        if (lclRgn != null) {
          lclRgn.waitOnInitialization();
          final long lastMod = gfc.cacheTimeMillis();
          Iterator it = this.ops.iterator();
          while (it.hasNext()) {
            QueuedOperation op = (QueuedOperation)it.next();
            op.process(lclRgn, getSender(), lastMod);
          }
        } else {
          ignored = true;
        }
      } catch (RegionDestroyedException e) {
        ignored = true;
      } catch (CancelException e) {
        ignored = true;
      } finally {
        ReplyMessage.send(getSender(), this.processorId, rex, dm, ignored, false, false);
      }
    }
    
    public int getDSFID() {
      return SEND_QUEUE_MESSAGE;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.regionPath = DataSerializer.readString(in);
      this.processorId = in.readInt();
      {
        int opCount = in.readInt();
        QueuedOperation[] ops = new QueuedOperation[opCount];
        for (int i=0; i < opCount; i++) {
          ops[i] = QueuedOperation.createFromData(in);
        }
        this.ops = Arrays.asList(ops);
      }
    }
    
    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeString(this.regionPath, out);
      out.writeInt(this.processorId);
      {
        int opCount = this.ops.size();
        out.writeInt(opCount);
        for (int i=0; i < opCount; i++) {
          QueuedOperation op = (QueuedOperation)this.ops.get(i);
          op.toData(out);
        }
      }
    }

    @Override
    public String toString() {
      StringBuffer buff = new StringBuffer();
      buff.append(getClass().getName());
      buff.append("(region path='"); // make sure this is the first one
      buff.append(this.regionPath);
      buff.append("'");
      buff.append("; processorId=");
      buff.append(this.processorId);
      buff.append("; queuedOps=");
      buff.append(this.ops.size());
      buff.append(")");
      return buff.toString();
    }
  }
}
