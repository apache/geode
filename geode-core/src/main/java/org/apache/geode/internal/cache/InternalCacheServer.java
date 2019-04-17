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

import java.io.IOException;
import java.util.List;

import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.security.SecurableCommunicationChannel;

public interface InternalCacheServer extends CacheServer {

  Acceptor getAcceptor();

  Acceptor createAcceptor(List overflowAttributesList) throws IOException;

  EndpointType getEndpointType();

  String getExternalAddress();

  enum EndpointType {
    GATEWAY(SecurableCommunicationChannel.GATEWAY, false, false),
    SERVER(SecurableCommunicationChannel.SERVER, true, true);

    private final SecurableCommunicationChannel channel;
    private final boolean inheritMembershipGroups;
    private final boolean notifyResourceEventListeners;

    EndpointType(SecurableCommunicationChannel channel, boolean inheritMembershipGroups,
        boolean notifyResourceEventListeners) {
      this.channel = channel;
      this.inheritMembershipGroups = inheritMembershipGroups;
      this.notifyResourceEventListeners = notifyResourceEventListeners;
    }

    public SecurableCommunicationChannel channel() {
      return channel;
    }

    public boolean notifyResourceEventListeners() {
      return notifyResourceEventListeners;
    }

    public boolean inheritMembershipGroups() {
      return inheritMembershipGroups;
    }
  }
}
