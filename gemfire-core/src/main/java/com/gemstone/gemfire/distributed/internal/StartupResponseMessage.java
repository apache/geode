/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.Instantiator;
//import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.InternalDataSerializer.SerializerAttributesHolder;
import com.gemstone.gemfire.internal.InternalInstantiator;
import com.gemstone.gemfire.internal.InternalInstantiator.InstantiatorAttributesHolder;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;

/**
 * A message that is sent to all other distribution manager when
 * a distribution manager starts up.
 */
public class StartupResponseMessage extends HighPriorityDistributionMessage implements AdminMessageType {
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
  protected int [] serializerIds = null;
  protected String[] serializerClasseNames = null;
  protected String[] instantiatorClasseNames = null;
  protected String[] instantiatedClasseNames = null;
  protected int [] instantiatorIds = null;
  protected transient StringBuffer fromDataProblems;

  public StartupResponseMessage() {
    
  }
  
  StartupResponseMessage(DistributionManager dm,
                                           int processorId,
                                           InternalDistributedMember recipient,
                                           String rejectionMessage,
                                           boolean responderIsAdmin) {
//    StartupResponseMessage m = new StartupResponseMessage();

    setRecipient(recipient);
    setProcessorId(processorId);
    this.rejectionMessage = rejectionMessage;
    this.responderIsAdmin = responderIsAdmin;

    // Note that if we can't read our network addresses, the peer will reject us.
    this.interfaces = StartupMessage.getMyAddresses(dm);
    this.distributedSystemId = dm.getDistributedSystemId();
    this.redundancyZone = dm.getRedundancyZone(dm.getId());
    
    /**
     * To fix B39705, we have added the instance variables to initialize the
     * information about the instantiators. While preparing the response message,
     * we populate this information. 
     **/
    // Fix for #43677
    Object[] instantiators = InternalInstantiator.getInstantiatorsForSerialization();
    this.instantiatorClasseNames = new String[instantiators.length];
    this.instantiatedClasseNames = new String[instantiators.length];
    this.instantiatorIds = new int[instantiators.length];
    for(int i=0; i< instantiators.length ; i++) {
      if (instantiators[i] instanceof Instantiator) {
        Instantiator inst = (Instantiator)instantiators[i];
        this.instantiatorClasseNames[i] = inst.getClass().getName();
        this.instantiatedClasseNames[i] = inst.getInstantiatedClass().getName();
        this.instantiatorIds[i] = inst.getId();
      } else {
        InstantiatorAttributesHolder inst = (InstantiatorAttributesHolder)instantiators[i];
        this.instantiatorClasseNames[i] = inst.getInstantiatorClassName();
        this.instantiatedClasseNames[i] = inst.getInstantiatedClassName();
        this.instantiatorIds[i] = inst.getId();
      }
    }

    SerializerAttributesHolder[] sahs = InternalDataSerializer.getSerializersForDistribution();
    this.serializerIds = new int[sahs.length];
    this.serializerClasseNames = new String[sahs.length];
    for(int i = 0; i < sahs.length ; i++ ) {
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
  public boolean sendViaJGroups() {
    return true;
  }
  
  /**
   * Adds the distribution managers that have started up to the current
   * DM's list of members.
   *
   * This method is invoked on the receiver side
   */
  @Override
  protected void process(DistributionManager dm) {

    if (this.interfaces == null || this.interfaces.size() == 0) {
//      this.rejectionMessage = "Peer " + getSender() + " has no network interfaces";
    }
    else {
      dm.setEquivalentHosts(this.interfaces);
    }
    dm.setDistributedSystemId(this.distributedSystemId);
    dm.setRedundancyZone(getSender(), this.redundancyZone);
    
    //Process the registration of instantiators & log failures, if any.
    if (this.fromDataProblems != null) {
      if (logger.isDebugEnabled()) {
        logger.debug(this.fromDataProblems);
      }
    }
    
    if(this.serializerIds != null) {
      for(int i = 0; i < serializerIds.length ; i++) {
        String cName = this.serializerClasseNames[i];
        if (cName != null) {
          InternalDataSerializer.register(cName, false, null, null, serializerIds[i]);  
        }        
      }
    }   
    
    if(this.instantiatorIds != null) {
      //Process the Instantiator registrations.
      for(int i=0; i < instantiatorIds.length ; i++) {
       String instantiatorClassName = instantiatorClasseNames[i];
       String instantiatedClassName = instantiatedClasseNames[i];
       int id = instantiatorIds[i];
       if ((instantiatorClassName != null) && (instantiatedClassName != null)) {
        InternalInstantiator.register(instantiatorClassName, instantiatedClassName, id, false); 
       }
      }      
    }
    
    dm.processStartupResponse(this.sender, this.rejectionMessage);

    StartupMessageReplyProcessor proc = (StartupMessageReplyProcessor)
        ReplyProcessor21.getProcessor(processorId);
    if (proc != null) {
      if (this.rejectionMessage != null) {
        // there's no reason to wait for other responses
        proc.setReceivedRejectionMessage(true); 
      }
      else {
        if (!this.responderIsAdmin) {
          proc.setReceivedAcceptance(true);
        }
        proc.process(this);
        if (logger.isTraceEnabled(LogMarker.DM)) {
          logger.trace(LogMarker.DM, "{} Processed {}", proc, this);
        }
      }
    } // proc != null
  }

  public int getDSFID() {
    return STARTUP_RESPONSE_MESSAGE;
  }
  
  @Override
  public void toData(DataOutput out) throws IOException {
    toDataContent(out, Version.CURRENT);
  }

  @Override
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    fromDataContent(in, Version.CURRENT);
  }
  
  private void fromDataProblem(String s) {
    if (this.fromDataProblems == null) {
      this.fromDataProblems = new StringBuffer();
    }

    this.fromDataProblems.append(s);
    this.fromDataProblems.append(System.getProperty("line.separator", "\n"));
  }

  // versions where serialization changed
  private static Version[] serializationVersions = new Version[] {
    Version.GFE_80, Version.GFE_82
  };
  
  @Override
  public Version[] getSerializationVersions() {
    return serializationVersions;
  }
  
  public void toDataContent(DataOutput out, Version ver) throws IOException {
    super.toData(out);
    out.writeInt(processorId);
    if (ver.compareTo(Version.GFE_80) < 0) {
      out.writeLong(System.currentTimeMillis());
    }
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
    if (ver.compareTo(Version.GFE_82) < 0) {
      DataSerializer.writeObject(new Properties(), out);
    }
    out.writeInt(distributedSystemId);
    DataSerializer.writeString(redundancyZone, out);
  }
  
  public void toDataPre_GFE_8_0_0_0(DataOutput out) throws IOException {
    toDataContent(out, Version.GFE_80);
  }

  public void toDataPre_GFE_8_2_0_0(DataOutput out) throws IOException {
    toDataContent(out, Version.GFE_82);
  }

  private void fromDataContent(DataInput in, Version ver)
    throws IOException, ClassNotFoundException {
      
    super.fromData(in);
    this.processorId = in.readInt();
    if (ver.compareTo(Version.GFE_80) < 0) {
      in.readLong();
    }
    this.rejectionMessage = DataSerializer.readString(in);
    this.responderIsAdmin = in.readBoolean();

    int serializerCount = in.readInt();
    this.serializerClasseNames = new String[serializerCount];
    this.serializerIds = new int[serializerCount];
    for (int i = 0; i < serializerCount; i++) {
      try {
        serializerClasseNames[i] = DataSerializer.readNonPrimitiveClassName(in);
      } 
      finally {
        serializerIds[i] = in.readInt(); // id  
      }
    }
    
    //Fix for B39705 : Deserialize the instantiators in the field variables.
    int instantiatorCount = in.readInt();
    instantiatorClasseNames = new String[instantiatorCount];
    instantiatedClasseNames = new String[instantiatorCount];
    instantiatorIds = new int[instantiatorCount];
    
    for (int i = 0; i < instantiatorCount; i++) {
      instantiatorClasseNames[i] = DataSerializer.readNonPrimitiveClassName(in);
      instantiatedClasseNames[i] = DataSerializer.readNonPrimitiveClassName(in);
      instantiatorIds[i] = in.readInt();
    } // for
    
    interfaces = (Set)DataSerializer.readObject(in);
    if (ver.compareTo(Version.GFE_82) < 0) {
      DataSerializer.readObject(in);
    }
    distributedSystemId = in.readInt();
    redundancyZone = DataSerializer.readString(in);
  }
 
  public void fromDataPre_GFE_8_0_0_0(DataInput in)
      throws IOException, ClassNotFoundException {
    fromDataContent(in, Version.GFE_80);
  }
  
  public void fromDataPre_GFE_8_2_0_0(DataInput in)
      throws IOException, ClassNotFoundException {
    fromDataContent(in, Version.GFE_82);
  }
  
  @Override
  public String toString() {
    return "StartupResponse: rejectionMessage="
          + this.rejectionMessage + " processor=" + processorId + " responderIsAdmin=" + this.responderIsAdmin + " distributed system id = " +this.distributedSystemId;
  }
}
