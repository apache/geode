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
   
   
package org.apache.geode.internal.admin.remote;

//import org.apache.geode.internal.admin.*;
import org.apache.geode.distributed.internal.*;
import org.apache.geode.*;
import org.apache.geode.cache.*;
import org.apache.geode.internal.*;
import org.apache.geode.internal.cache.*;
import java.io.*;
//import java.net.*;
//import java.util.*;
import org.apache.geode.distributed.internal.membership.*;

/**
 * A message that is sent in response to a {@link CacheConfigRequest}.
 * @since GemFire 3.5
 */
public final class CacheConfigResponse extends AdminResponse {
  // instance variables
  private RemoteCacheInfo info;
  
  /** An exception thrown while configuring the cache
   *
   * @since GemFire 4.0 */
  private Exception exception;
  
  /**
   * Returns a <code>CacheConfigResponse</code> that will be returned to the
   * specified recipient.
   */
  public static CacheConfigResponse create(DistributionManager dm, InternalDistributedMember recipient, int cacheId, byte attributeCode, int newValue) {
    CacheConfigResponse m = new CacheConfigResponse();
    m.setRecipient(recipient);
    try {
      GemFireCacheImpl c = (GemFireCacheImpl)CacheFactory.getInstanceCloseOk(dm.getSystem());
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
          Assert.assertTrue(false, "Unknown config code: " +
                            attributeCode);
        }
      }
      m.info = new RemoteCacheInfo(c);
    } 
    catch (CancelException ex) {
      m.info = null;

    } catch (Exception ex) {
      m.exception = ex;
      m.info = null;
    }
    return m;
  }

  public RemoteCacheInfo getCacheInfo() {
    return this.info;
  }
  
  /**
   * Returns the exception that was thrown while changing the cache
   * configuration.
   */
  public Exception getException() {
    return this.exception;
  }

  public int getDSFID() {
    return CACHE_CONFIG_RESPONSE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.info, out);
    DataSerializer.writeObject(this.exception, out);
  }

  @Override
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.info = (RemoteCacheInfo)DataSerializer.readObject(in);
    this.exception = (Exception) DataSerializer.readObject(in);
  }

  @Override
  public String toString() {
    return "CacheConfigResponse from " + this.getSender() + " info=" + this.info;
  }
}
