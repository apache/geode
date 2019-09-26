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
package org.apache.geode.distributed.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.Instantiator;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.InternalDataSerializer.SerializerAttributesHolder;
import org.apache.geode.internal.InternalInstantiator;
import org.apache.geode.internal.InternalInstantiator.InstantiatorAttributesHolder;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.Version;

/**
 * A message that is sent to all other distribution manager when a distribution manager starts up.
 */
public class StartupResponseMessage extends DistributionMessage
    implements AdminMessageType {
  private static final Logger logger = LogService.getLogger();

  /** The current cache time of the sending DM */
  protected String rejectionMessage;
  protected int processorId;
  protected boolean responderIsAdmin;
  protected Set interfaces;
  protected int distributedSystemId;
  protected String redundancyZone;

  /**
   * To fix B39705, added the instance variables for storing instantiator information.
   **/
  protected int[] serializerIds = null;
  protected String[] serializerClasseNames = null;
  protected String[] instantiatorClasseNames = null;
  protected String[] instantiatedClasseNames = null;
  protected int[] instantiatorIds = null;
  protected transient StringBuffer fromDataProblems;

  public StartupResponseMessage() {

  }

  StartupResponseMessage(ClusterDistributionManager dm, int processorId,
      InternalDistributedMember recipient, String rejectionMessage, boolean responderIsAdmin) {
    // StartupResponseMessage m = new StartupResponseMessage();

    setRecipient(recipient);
    setProcessorId(processorId);
    this.rejectionMessage = rejectionMessage;
    this.responderIsAdmin = responderIsAdmin;

    // Note that if we can't read our network addresses, the peer will reject us.
    this.interfaces = StartupMessage.getMyAddresses(dm);
    this.distributedSystemId = dm.getDistributedSystemId();
    this.redundancyZone = dm.getRedundancyZone(dm.getId());

    /**
     * To fix B39705, we have added the instance variables to initialize the information about the
     * instantiators. While preparing the response message, we populate this information.
     **/
    // Fix for #43677
    Object[] instantiators = InternalInstantiator.getInstantiatorsForSerialization();
    this.instantiatorClasseNames = new String[instantiators.length];
    this.instantiatedClasseNames = new String[instantiators.length];
    this.instantiatorIds = new int[instantiators.length];
    for (int i = 0; i < instantiators.length; i++) {
      if (instantiators[i] instanceof Instantiator) {
        Instantiator inst = (Instantiator) instantiators[i];
        this.instantiatorClasseNames[i] = inst.getClass().getName();
        this.instantiatedClasseNames[i] = inst.getInstantiatedClass().getName();
        this.instantiatorIds[i] = inst.getId();
      } else {
        InstantiatorAttributesHolder inst = (InstantiatorAttributesHolder) instantiators[i];
        this.instantiatorClasseNames[i] = inst.getInstantiatorClassName();
        this.instantiatedClasseNames[i] = inst.getInstantiatedClassName();
        this.instantiatorIds[i] = inst.getId();
      }
    }

    SerializerAttributesHolder[] sahs = InternalDataSerializer.getSerializersForDistribution();
    this.serializerIds = new int[sahs.length];
    this.serializerClasseNames = new String[sahs.length];
    for (int i = 0; i < sahs.length; i++) {
      this.serializerIds[i] = sahs[i].getId();
      this.serializerClasseNames[i] = sahs[i].getClassName();
    }
  }

  /**
   * set the processor id for this message
   */
  public void setProcessorId(int processorId) {
    this.processorId = processorId;
  }

  /** replymessages are always processed in-line */
  @Override
  public boolean getInlineProcess() {
    return true;
  }

  @Override
  public int getProcessorType() {
    return OperationExecutors.WAITING_POOL_EXECUTOR;
  }

  @Override
  public boolean sendViaUDP() {
    return true;
  }

  /**
   * Adds the distribution managers that have started up to the current DM's list of members.
   *
   * This method is invoked on the receiver side
   */
  @Override
  protected void process(ClusterDistributionManager dm) {

    if (this.interfaces == null || this.interfaces.size() == 0) {
      // this.rejectionMessage = "Peer " + getSender() + " has no network interfaces";
    } else {
      dm.setEquivalentHosts(this.interfaces);
    }
    dm.setDistributedSystemId(this.distributedSystemId);
    dm.setRedundancyZone(getSender(), this.redundancyZone);

    // Process the registration of instantiators & log failures, if any.
    if (this.fromDataProblems != null) {
      if (logger.isDebugEnabled()) {
        logger.debug(this.fromDataProblems);
      }
    }

    if (this.serializerIds != null) {
      for (int i = 0; i < serializerIds.length; i++) {
        String cName = this.serializerClasseNames[i];
        if (cName != null) {
          InternalDataSerializer.register(cName, false, null, null, serializerIds[i]);
        }
      }
    }

    if (this.instantiatorIds != null) {
      // Process the Instantiator registrations.
      for (int i = 0; i < instantiatorIds.length; i++) {
        String instantiatorClassName = instantiatorClasseNames[i];
        String instantiatedClassName = instantiatedClasseNames[i];
        int id = instantiatorIds[i];
        if ((instantiatorClassName != null) && (instantiatedClassName != null)) {
          InternalInstantiator.register(instantiatorClassName, instantiatedClassName, id, false);
        }
      }
    }

    dm.processStartupResponse(this.sender, this.rejectionMessage);

    StartupMessageReplyProcessor proc =
        (StartupMessageReplyProcessor) ReplyProcessor21.getProcessor(processorId);
    if (proc != null) {
      if (this.rejectionMessage != null) {
        // there's no reason to wait for other responses
        proc.setReceivedRejectionMessage(true);
      } else {
        if (!this.responderIsAdmin) {
          proc.setReceivedAcceptance(true);
        }
      }

      proc.process(this);
      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE, "{} Processed {}", proc, this);
      }
    } // proc != null
  }

  @Override
  public int getDSFID() {
    return STARTUP_RESPONSE_MESSAGE;
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {

    super.toData(out, context);

    out.writeInt(processorId);
    DataSerializer.writeString(this.rejectionMessage, out);
    out.writeBoolean(this.responderIsAdmin);

    // Send a description of all of the DataSerializers and
    // Instantiators that have been registered
    out.writeInt(serializerIds.length);
    for (int i = 0; i < serializerIds.length; i++) {
      DataSerializer.writeNonPrimitiveClassName(serializerClasseNames[i], out);
      out.writeInt(serializerIds[i]);
    }

    out.writeInt(this.instantiatorIds.length);
    for (int i = 0; i < instantiatorIds.length; i++) {
      DataSerializer.writeNonPrimitiveClassName(this.instantiatorClasseNames[i], out);
      DataSerializer.writeNonPrimitiveClassName(this.instantiatedClasseNames[i], out);
      out.writeInt(this.instantiatorIds[i]);
    }

    DataSerializer.writeObject(interfaces, out);
    out.writeInt(distributedSystemId);
    DataSerializer.writeString(redundancyZone, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {

    super.fromData(in, context);

    this.processorId = in.readInt();
    this.rejectionMessage = DataSerializer.readString(in);
    this.responderIsAdmin = in.readBoolean();

    int serializerCount = in.readInt();
    this.serializerClasseNames = new String[serializerCount];
    this.serializerIds = new int[serializerCount];
    for (int i = 0; i < serializerCount; i++) {
      try {
        serializerClasseNames[i] = DataSerializer.readNonPrimitiveClassName(in);
      } finally {
        serializerIds[i] = in.readInt(); // id
      }
    }

    // Fix for B39705 : Deserialize the instantiators in the field variables.
    int instantiatorCount = in.readInt();
    instantiatorClasseNames = new String[instantiatorCount];
    instantiatedClasseNames = new String[instantiatorCount];
    instantiatorIds = new int[instantiatorCount];

    for (int i = 0; i < instantiatorCount; i++) {
      instantiatorClasseNames[i] = DataSerializer.readNonPrimitiveClassName(in);
      instantiatedClasseNames[i] = DataSerializer.readNonPrimitiveClassName(in);
      instantiatorIds[i] = in.readInt();
    } // for

    interfaces = (Set) DataSerializer.readObject(in);
    distributedSystemId = in.readInt();
    redundancyZone = DataSerializer.readString(in);
  }

  @Override
  public String toString() {
    return "StartupResponse: rejectionMessage=" + this.rejectionMessage + " processor="
        + processorId + " responderIsAdmin=" + this.responderIsAdmin + " distributed system id = "
        + this.distributedSystemId;
  }
}
