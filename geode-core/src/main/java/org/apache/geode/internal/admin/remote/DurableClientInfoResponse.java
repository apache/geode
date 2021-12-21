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
package org.apache.geode.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A message that is sent in response to a {@link DurableClientInfoRequest}.
 *
 * @since GemFire 5.6
 */
public class DurableClientInfoResponse extends AdminResponse {
  // instance variables
  /**
   * The result for the query made the request (hasDurableClient or isPrimaryForDurableClient).
   */
  private boolean returnVal = false;

  /**
   * Returns a {@code DurableClientInfoResponse} that will be returned to the specified recipient.
   */
  public static DurableClientInfoResponse create(DistributionManager dm,
      InternalDistributedMember recipient, DurableClientInfoRequest request) {
    DurableClientInfoResponse m = new DurableClientInfoResponse();
    m.setRecipient(recipient);
    try {
      InternalCache c = (InternalCache) CacheFactory.getInstanceCloseOk(dm.getSystem());
      if (!c.getCacheServers().isEmpty()) {

        CacheServerImpl server = (CacheServerImpl) c.getCacheServers().iterator().next();
        switch (request.action) {
          case DurableClientInfoRequest.HAS_DURABLE_CLIENT_REQUEST: {
            m.returnVal =
                server.getAcceptor().getCacheClientNotifier().hasDurableClient(request.durableId);
            break;
          }
          case DurableClientInfoRequest.IS_PRIMARY_FOR_DURABLE_CLIENT_REQUEST: {
            m.returnVal = server.getAcceptor().getCacheClientNotifier()
                .hasPrimaryForDurableClient(request.durableId);
            break;
          }
        }
      }
    } catch (CacheClosedException ignore) {
      // do nothing
    }
    return m;
  }

  boolean getResultBoolean() {
    return returnVal;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeBoolean(returnVal);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    returnVal = in.readBoolean();
  }

  @Override
  public String toString() {
    return "DurableClientInfoResponse from " + getSender();
  }

  @Override
  public int getDSFID() {
    return DURABLE_CLIENT_INFO_RESPONSE;
  }
}
