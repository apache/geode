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
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A message that is sent in response to a {@link CacheConfigRequest}.
 *
 * @since GemFire 3.5
 */
public class CacheConfigResponse extends AdminResponse {

  private RemoteCacheInfo info;

  /**
   * An exception thrown while configuring the cache
   *
   * @since GemFire 4.0
   */
  private Exception exception;

  /**
   * Returns a {@code CacheConfigResponse} that will be returned to the specified recipient.
   */
  public static CacheConfigResponse create(DistributionManager dm,
      InternalDistributedMember recipient, int cacheId, byte attributeCode, int newValue) {
    CacheConfigResponse m = new CacheConfigResponse();
    m.setRecipient(recipient);
    try {
      InternalCache c = (InternalCache) CacheFactory.getInstanceCloseOk(dm.getSystem());
      if (cacheId != System.identityHashCode(c)) {
        m.info = null;
      } else {
        switch (attributeCode) {
          case RemoteGemFireVM.LOCK_TIMEOUT_CODE:
            c.setLockTimeout(newValue);
            break;
          case RemoteGemFireVM.LOCK_LEASE_CODE:
            c.setLockLease(newValue);
            break;
          case RemoteGemFireVM.SEARCH_TIMEOUT_CODE:
            c.setSearchTimeout(newValue);
            break;
          default:
            Assert.assertTrue(false, "Unknown config code: " + attributeCode);
        }
      }
      m.info = new RemoteCacheInfo(c);
    } catch (CancelException ignore) {
      m.info = null;

    } catch (Exception ex) {
      m.exception = ex;
      m.info = null;
    }
    return m;
  }

  RemoteCacheInfo getCacheInfo() {
    return info;
  }

  /**
   * Returns the exception that was thrown while changing the cache configuration.
   */
  public Exception getException() {
    return exception;
  }

  @Override
  public int getDSFID() {
    return CACHE_CONFIG_RESPONSE;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeObject(info, out);
    DataSerializer.writeObject(exception, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    info = DataSerializer.readObject(in);
    exception = DataSerializer.readObject(in);
  }

  @Override
  public String toString() {
    return "CacheConfigResponse from " + getSender() + " info=" + info;
  }
}
