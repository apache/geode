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
import org.apache.geode.InternalGemFireException;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.GemFireVM;

/**
 * Responds to {@link RegionResponse}.
 *
 * @since GemFire 3.5
 */
public class RegionResponse extends AdminResponse {
  // instance variables

  /** The name of the region returned in the response. */
  private String name;

  /** The user attribute for the region */
  private String userAttribute;

  /** An exception that occurred while generating this response */
  private Exception exception;

  /////////////////////// Static Methods ///////////////////////

  /**
   * Returns a <code>RegionResponse</code> that will be returned to the specified recipient. The
   * message will contains a copy of the local manager's system config.
   */
  public static RegionResponse create(DistributionManager dm, InternalDistributedMember recipient,
      RegionRequest request) {
    RegionResponse m = new RegionResponse();

    try {
      Cache cache = CacheFactory.getInstance(dm.getSystem());

      int cacheId = request.cacheId;
      if (System.identityHashCode(cache) == cacheId) {
        Region r;
        int action = request.action;
        switch (action) {
          case RegionRequest.GET_REGION:
            r = cache.getRegion(request.path);
            break;

          case RegionRequest.CREATE_VM_ROOT:
            r = cache.createRegion(request.newRegionName, request.newRegionAttributes);
            break;

          case RegionRequest.CREATE_VM_REGION:
            Region parent = cache.getRegion(request.path);
            r = parent.createSubregion(request.newRegionName, request.newRegionAttributes);
            break;

          default:
            throw new InternalGemFireException(
                String.format("Unknown RegionRequest operation: %s",
                    Integer.valueOf(action)));
        }

        if (r != null) {
          m.name = r.getFullPath();
          m.userAttribute = (String) CacheDisplay.getCachedObjectDisplay(r.getUserAttribute(),
              GemFireVM.LIGHTWEIGHT_CACHE_VALUE);

        } else {
          m.name = null;
        }
      }
    } catch (CancelException cce) {
      /* no cache yet */

    } catch (Exception ex) {
      // Something went wrong while creating the region
      m.exception = ex;
    }
    m.setRecipient(recipient);
    return m;
  }

  // instance methods

  public Region getRegion(RemoteGemFireVM vm) {
    if (this.name == null) {
      return null;
    } else {
      return new AdminRegion(this.name, vm, this.userAttribute);
    }
  }

  /**
   * Returns any exception that was thrown while generating this response.
   */
  public Exception getException() {
    return this.exception;
  }

  public int getDSFID() {
    return REGION_RESPONSE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeString(this.name, out);
    DataSerializer.writeString(this.userAttribute, out);
    DataSerializer.writeObject(this.exception, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.name = DataSerializer.readString(in);
    this.userAttribute = DataSerializer.readString(in);
    this.exception = (Exception) DataSerializer.readObject(in);
  }

  @Override
  public String toString() {
    return "RegionResponse from " + this.getRecipient();
  }
}
