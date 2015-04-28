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
import java.util.Collection;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * @author xzhou
 * @since 6.6.2
 */
public class StartupResponseWithVersionMessage extends StartupResponseMessage {
  private static final Logger logger = LogService.getLogger();
  
  private String version; // added for bug 43945

  // additional fields using StartupMessageData below here...
  private Collection<String> hostedLocators;
  private boolean isSharedConfigurationEnabled;
  
  public StartupResponseWithVersionMessage() {
    
  }
  
  StartupResponseWithVersionMessage(DistributionManager dm,
      int processorId,
      InternalDistributedMember recipient,
      String rejectionMessage,
      boolean responderIsAdmin) {
    super(dm, processorId, recipient, rejectionMessage, responderIsAdmin);
    version = GemFireVersion.getGemFireVersion();
    this.hostedLocators = InternalLocator.getLocatorStrings();
    InternalLocator locator = InternalLocator.getLocator();
    if (locator != null) {
      this.isSharedConfigurationEnabled = locator.isSharedConfigurationEnabled();
    }
  }
  
  @Override
  protected void process(DistributionManager dm) {
    super.process(dm);
    if (this.hostedLocators != null) {
      dm.addHostedLocators(getSender(), this.hostedLocators, this.isSharedConfigurationEnabled);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Received StartupResponseWithVersionMessage from a member with version: {}", this.version);
    }
  }

  public int getDSFID() {
    return STARTUP_RESPONSE_WITHVERSION_MESSAGE;
  }

  @Override
  public String toString() {
    return super.toString() + " version="+this.version;
  }

  // versions where serialization changed
  private static Version[] serializationVersions = new Version[] {
    Version.GFE_80};
  
  @Override
  public Version[] getSerializationVersions() {
    return serializationVersions;
  }

  public void toDataPre_GFE_8_0_0_0(DataOutput out) throws IOException {
    super.toDataPre_GFE_8_0_0_0(out);
    cmnToData(out);
  }
  
  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    cmnToData(out);
  }

  public void cmnToData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.version, out);
    
    StartupMessageData data = new StartupMessageData();
    data.writeHostedLocators(this.hostedLocators);
    data.writeIsSharedConfigurationEnabled(this.isSharedConfigurationEnabled);
    data.toData(out);
  }

  @Override
  public void fromDataPre_GFE_8_0_0_0(DataInput in) throws IOException, ClassNotFoundException {
    super.fromDataPre_GFE_8_0_0_0(in);
    cmnFromData(in);
  }
  
  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    cmnFromData(in);
  }

  public void cmnFromData(DataInput in) throws IOException, ClassNotFoundException {
    this.version = DataSerializer.readString(in);
    
    StartupMessageData data = new StartupMessageData(in, this.version);
    this.hostedLocators = data.readHostedLocators();
    this.isSharedConfigurationEnabled = data.readIsSharedConfigurationEnabled();
  }
}
