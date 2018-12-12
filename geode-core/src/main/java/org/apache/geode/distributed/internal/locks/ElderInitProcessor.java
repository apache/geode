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

package org.apache.geode.distributed.internal.locks;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;

/**
 * A processor for initializing the ElderState. This may involve sending a message to every existing
 * member to discover what services they have.
 *
 * @since GemFire 4.0
 */
public class ElderInitProcessor extends ReplyProcessor21 {
  private static final Logger logger = LogService.getLogger();

  private final HashMap grantors;
  private final HashSet crashedGrantors;

  ////////// Public static entry point /////////


  /**
   * Initializes ElderState map by recovering all existing grantors and crashed grantors in the
   * current ds.
   */
  static void init(DistributionManager dm, HashMap<String, GrantorInfo> map) {
    HashSet<String> crashedGrantors = new HashSet<>();
    Set others = dm.getOtherDistributionManagerIds();
    if (!others.isEmpty()) {
      ElderInitProcessor processor = new ElderInitProcessor(dm, others, map, crashedGrantors);
      ElderInitMessage.send(others, dm, processor);
      try {
        processor.waitForRepliesUninterruptibly();
      } catch (ReplyException e) {
        e.handleCause();
      }
    }
    // always recover from ourself
    GrantorRequestProcessor.readyForElderRecovery(dm.getSystem(), null, null);
    DLockService.recoverLocalElder(dm, map, crashedGrantors);

    for (String crashedGrantor : crashedGrantors) {
      map.put(crashedGrantor, new GrantorInfo(null, 0, 0, true));
    }
  }
  //////////// Instance methods //////////////

  /**
   * Creates a new instance of ElderInitProcessor
   */
  private ElderInitProcessor(DistributionManager dm, Set others, HashMap grantors,
      HashSet crashedGrantors) {
    super(dm/* fix bug 33297 */, others);
    this.grantors = grantors;
    this.crashedGrantors = crashedGrantors;
  }

  /**
   * Note the synchronization; we can only process one response at a time.
   */
  private synchronized void processData(ArrayList rmtGrantors, ArrayList rmtGrantorVersions,
      ArrayList rmtGrantorSerialNumbers, ArrayList rmtNonGrantors,
      InternalDistributedMember rmtId) {
    {
      Iterator iterGrantorServices = rmtGrantors.iterator();
      Iterator iterGrantorVersions = rmtGrantorVersions.iterator();
      Iterator iterGrantorSerialNumbers = rmtGrantorSerialNumbers.iterator();
      while (iterGrantorServices.hasNext()) {
        String serviceName = (String) iterGrantorServices.next();
        long versionId = ((Long) iterGrantorVersions.next()).longValue();
        int serialNumber = ((Integer) iterGrantorSerialNumbers.next()).intValue();
        GrantorInfo oldgi = (GrantorInfo) this.grantors.get(serviceName);
        if (oldgi == null || oldgi.getVersionId() < versionId) {
          this.grantors.put(serviceName, new GrantorInfo(rmtId, versionId, serialNumber, false));
          this.crashedGrantors.remove(serviceName);
        }
      }
    }
    {
      Iterator it = rmtNonGrantors.iterator();
      while (it.hasNext()) {
        String serviceName = (String) it.next();
        if (!this.grantors.containsKey(serviceName)) {
          this.crashedGrantors.add(serviceName);
        }
      }
    }
  }

  @Override
  public void process(DistributionMessage msg) {
    if (msg instanceof ElderInitReplyMessage) {
      ElderInitReplyMessage eiMsg = (ElderInitReplyMessage) msg;
      processData(eiMsg.getGrantors(), eiMsg.getGrantorVersions(), eiMsg.getGrantorSerialNumbers(),
          eiMsg.getNonGrantors(), eiMsg.getSender());
    } else {
      Assert.assertTrue(false,
          "Expected instance of ElderInitReplyMessage but got " + msg.getClass());
    }
    super.process(msg);
  }

  /////////////// Inner message classes //////////////////

  public static class ElderInitMessage extends PooledDistributionMessage
      implements MessageWithReply {
    private int processorId;

    protected static void send(Set others, DistributionManager dm, ReplyProcessor21 proc) {
      ElderInitMessage msg = new ElderInitMessage();
      msg.processorId = proc.getProcessorId();
      msg.setRecipients(others);
      if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
        logger.trace(LogMarker.DLS_VERBOSE, "ElderInitMessage sending {} to {}", msg, others);
      }
      dm.putOutgoing(msg);
    }

    @Override
    public int getProcessorId() {
      return this.processorId;
    }

    private void reply(DistributionManager dm, ArrayList grantors, ArrayList grantorVersions,
        ArrayList grantorSerialNumbers, ArrayList nonGrantors) {
      ElderInitReplyMessage.send(this, dm, grantors, grantorVersions, grantorSerialNumbers,
          nonGrantors);
    }

    @Override
    protected void process(ClusterDistributionManager dm) {
      ArrayList grantors = new ArrayList(); // svc names grantor for
      ArrayList grantorVersions = new ArrayList(); // grantor versions
      ArrayList grantorSerialNumbers = new ArrayList(); // serial numbers of grantor svcs
      ArrayList nonGrantors = new ArrayList(); // svc names non-grantor for
      if (dm.waitForElder(this.getSender())) {
        GrantorRequestProcessor.readyForElderRecovery(dm.getSystem(), this.getSender(), null);
        DLockService.recoverRmtElder(grantors, grantorVersions, grantorSerialNumbers, nonGrantors);
        reply(dm, grantors, grantorVersions, grantorSerialNumbers, nonGrantors);
      } else if (dm.getOtherNormalDistributionManagerIds().isEmpty()) {
        // Either we're alone (and received a message from an unknown member) or else we haven't
        // yet processed a view. In either case, we clearly don't have any grantors,
        // so we return empty lists.

        logger.info(LogMarker.DLS_MARKER,
            "{}: returning empty lists because I know of no other members.",
            this);
        reply(dm, grantors, grantorVersions, grantorSerialNumbers, nonGrantors);
      } else {
        logger.info(LogMarker.DLS_MARKER, "{}: disregarding request from departed member.", this);
      }
    }

    public int getDSFID() {
      return ELDER_INIT_MESSAGE;
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

    @Override
    public String toString() {
      StringBuffer buff = new StringBuffer();
      buff.append("ElderInitMessage (processorId='").append(this.processorId).append(")");
      return buff.toString();
    }
  }

  public static class ElderInitReplyMessage extends ReplyMessage {
    private ArrayList grantors; // svc names
    private ArrayList grantorVersions; // grantor version longs
    private ArrayList grantorSerialNumbers; // grantor dls serial number ints
    private ArrayList nonGrantors; // svc names

    public static void send(MessageWithReply reqMsg, DistributionManager dm, ArrayList grantors,
        ArrayList grantorVersions, ArrayList grantorSerialNumbers, ArrayList nonGrantors) {
      ElderInitReplyMessage m = new ElderInitReplyMessage();
      m.grantors = grantors;
      m.grantorVersions = grantorVersions;
      m.grantorSerialNumbers = grantorSerialNumbers;
      m.nonGrantors = nonGrantors;
      m.processorId = reqMsg.getProcessorId();
      m.setRecipient(reqMsg.getSender());
      dm.putOutgoing(m);
    }

    public ArrayList getGrantors() {
      return this.grantors;
    }

    public ArrayList getGrantorVersions() {
      return this.grantorVersions;
    }

    public ArrayList getGrantorSerialNumbers() {
      return this.grantorSerialNumbers;
    }

    public ArrayList getNonGrantors() {
      return this.nonGrantors;
    }

    @Override
    public int getDSFID() {
      return ELDER_INIT_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.grantors = DataSerializer.readArrayList(in);
      this.grantorVersions = DataSerializer.readArrayList(in);
      this.grantorSerialNumbers = DataSerializer.readArrayList(in);
      this.nonGrantors = DataSerializer.readArrayList(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeArrayList(this.grantors, out);
      DataSerializer.writeArrayList(this.grantorVersions, out);
      DataSerializer.writeArrayList(this.grantorSerialNumbers, out);
      DataSerializer.writeArrayList(this.nonGrantors, out);
    }

    @Override
    public String toString() {
      StringBuffer buff = new StringBuffer();
      buff.append("ElderInitReplyMessage").append("; sender=").append(getSender())
          .append("; processorId=").append(super.processorId).append("; grantors=")
          .append(this.grantors).append("; grantorVersions=").append(this.grantorVersions)
          .append("; grantorSerialNumbers=").append(this.grantorSerialNumbers)
          .append("; nonGrantors=").append(this.nonGrantors).append(")");
      return buff.toString();
    }
  }
}
