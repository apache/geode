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

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A message that is sent in response to a {@link CacheInfoRequest}.
 *
 * @since GemFire 3.5
 */
public class CacheInfoResponse extends AdminResponse {

  private RemoteCacheInfo info;

  /**
   * Returns a {@code CacheInfoResponse} that will be returned to the specified recipient.
   */
  public static CacheInfoResponse create(DistributionManager dm,
      InternalDistributedMember recipient) {
    CacheInfoResponse m = new CacheInfoResponse();
    m.setRecipient(recipient);
    try {
      InternalCache c = (InternalCache) CacheFactory.getInstanceCloseOk(dm.getSystem());
      m.info = new RemoteCacheInfo(c);
    } catch (CancelException ignore) {
      m.info = null;
    }
    return m;
  }

  RemoteCacheInfo getCacheInfo() {
    return info;
  }

  @Override
  public int getDSFID() {
    return CACHE_INFO_RESPONSE;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeObject(info, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    info = DataSerializer.readObject(in);
  }

  @Override
  public String toString() {
    return "CacheInfoResponse from " + getSender() + " info=" + info;
  }
}
