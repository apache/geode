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
import java.util.Collection;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

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

  StartupResponseWithVersionMessage(ClusterDistributionManager dm, int processorId,
      InternalDistributedMember recipient, String rejectionMessage, boolean responderIsAdmin) {
    super(dm, processorId, recipient, rejectionMessage, responderIsAdmin);
    version = GemFireVersion.getGemFireVersion();
    hostedLocators = InternalLocator.getLocatorStrings();
    InternalLocator locator = InternalLocator.getLocator();
    if (locator != null) {
      isSharedConfigurationEnabled = locator.isSharedConfigurationEnabled();
    }
  }

  @Override
  protected void process(ClusterDistributionManager dm) {
    if (hostedLocators != null) {
      dm.addHostedLocators(getSender(), hostedLocators, isSharedConfigurationEnabled);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Received StartupResponseWithVersionMessage from a member with version: {}",
          version);
    }
    super.process(dm);
  }

  @Override
  public int getDSFID() {
    return STARTUP_RESPONSE_WITHVERSION_MESSAGE;
  }

  @Override
  public String toString() {
    return super.toString() + " version=" + version;
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeString(version, out);
    StartupMessageData data = new StartupMessageData();
    data.writeHostedLocators(hostedLocators);
    data.writeIsSharedConfigurationEnabled(isSharedConfigurationEnabled);
    data.writeTo(out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    version = DataSerializer.readString(in);
    StartupMessageData data = new StartupMessageData();
    data.readFrom(in);
    hostedLocators = data.readHostedLocators();
    isSharedConfigurationEnabled = data.readIsSharedConfigurationEnabled();
  }
}
