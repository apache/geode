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

import org.apache.geode.distributed.internal.DistributionManager;

/**
 * A message that is sent to a particular distribution manager to get information on its current
 * cache.
 *
 * @since GemFire 3.5
 */
public class CacheInfoRequest extends AdminRequest {
  /**
   * Returns a <code>CacheInfoRequest</code>.
   */
  public static CacheInfoRequest create() {
    CacheInfoRequest m = new CacheInfoRequest();
    return m;
  }

  public CacheInfoRequest() {
    friendlyName = "Fetch cache up time";
  }

  /**
   * Must return a proper response to this request.
   *
   */
  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    return CacheInfoResponse.create(dm, this.getSender());
  }

  public int getDSFID() {
    return CACHE_INFO_REQUEST;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
  }

  @Override
  public String toString() {
    return "CacheInfoRequest from " + this.getSender();
  }
}
