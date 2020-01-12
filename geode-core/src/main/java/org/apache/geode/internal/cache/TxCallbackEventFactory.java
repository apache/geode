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
package org.apache.geode.internal.cache;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.offheap.annotations.Retained;

/**
 * Factory for creating instances of transaction callback event
 */
public interface TxCallbackEventFactory {
  @Retained
  EntryEventImpl createCallbackEvent(InternalRegion internalRegion,
      Operation op, Object key, Object newValue,
      TransactionId txId, TXRmtEvent txEvent,
      EventID eventId, Object aCallbackArgument,
      FilterRoutingInfo filterRoutingInfo,
      ClientProxyMembershipID bridgeContext,
      TXEntryState txEntryState, VersionTag versionTag,
      long tailKey);
}
