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
package com.gemstone.gemfire.distributed.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * @since GemFire 6.6.2
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

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeString(this.version, out);
    StartupMessageData data = new StartupMessageData();
    data.writeHostedLocators(this.hostedLocators);
    data.writeIsSharedConfigurationEnabled(this.isSharedConfigurationEnabled);
    data.writeTo(out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.version = DataSerializer.readString(in);
    StartupMessageData data = new StartupMessageData();
    data.readFrom(in);
    this.hostedLocators = data.readHostedLocators();
    this.isSharedConfigurationEnabled = data.readIsSharedConfigurationEnabled();
  }
}
