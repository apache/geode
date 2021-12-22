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

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.admin.CacheInfo;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A message that is sent to a particular application to get the region for the specified path.
 *
 * @since GemFire 3.5
 */
public class RegionRequest extends AdminRequest {

  /** Request to get a region */
  static final int GET_REGION = 10;

  /** Request to create a VM root region */
  static final int CREATE_VM_ROOT = 11;

  /** Request to create a VM region */
  static final int CREATE_VM_REGION = 12;

  ////////////////////// Instance Fields //////////////////////

  /** The action to be taken by this request */
  int action = 0;

  /** The id of the Cache in the recipient VM */
  int cacheId = 0;

  /** The path to the region requested or operated on */
  String path;

  /** The name of region to create */
  String newRegionName;

  /** The attributes for the region to create */
  RegionAttributes newRegionAttributes;

  ////////////////////// Static Methods ///////////////////////

  /**
   * Returns a <code>RegionRequest</code> for getting a region with the given name.
   *
   * @param c The admin object for the remote cache
   * @param path The full path to the region
   */
  public static RegionRequest createForGet(CacheInfo c, String path) {
    RegionRequest m = new RegionRequest();
    m.action = GET_REGION;
    m.cacheId = c.getId();
    m.path = path;
    RegionRequest.setFriendlyName(m);
    return m;
  }

  /**
   * Returns a <code>RegionRequest</code> for creating a VM root region with the given name and
   * attributes.
   */
  public static RegionRequest createForCreateRoot(CacheInfo c, String name,
      RegionAttributes attrs) {
    RegionRequest m = new RegionRequest();
    m.action = CREATE_VM_ROOT;
    m.cacheId = c.getId();
    m.newRegionName = name;
    m.newRegionAttributes = new RemoteRegionAttributes(attrs);
    RegionRequest.setFriendlyName(m);
    return m;
  }

  /**
   * Returns a <code>RegionRequest</code> for creating a VM root region with the given name and
   * attributes.
   */
  public static RegionRequest createForCreateSubregion(CacheInfo c, String parentPath, String name,
      RegionAttributes attrs) {
    RegionRequest m = new RegionRequest();
    m.action = CREATE_VM_REGION;
    m.cacheId = c.getId();
    m.path = parentPath;
    m.newRegionName = name;
    m.newRegionAttributes = new RemoteRegionAttributes(attrs);
    RegionRequest.setFriendlyName(m);
    return m;
  }

  public RegionRequest() {
    RegionRequest.setFriendlyName(this);
  }

  /**
   * Must return a proper response to this request.
   *
   */
  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    // nothing needs to be done. If we got this far then a cache must exist.
    return RegionResponse.create(dm, getSender(), this);
  }

  @Override
  public int getDSFID() {
    return REGION_REQUEST;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeInt(action);
    out.writeInt(cacheId);
    DataSerializer.writeString(path, out);
    DataSerializer.writeString(newRegionName, out);
    DataSerializer.writeObject(newRegionAttributes, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    action = in.readInt();
    cacheId = in.readInt();
    path = DataSerializer.readString(in);
    newRegionName = DataSerializer.readString(in);
    newRegionAttributes = DataSerializer.readObject(in);
    RegionRequest.setFriendlyName(this);
  }

  @Override
  public String toString() {
    return "RegionRequest from " + getRecipient() + " path=" + path;
  }

  private static void setFriendlyName(RegionRequest rgnRqst) {
    switch (rgnRqst.action) {
      case GET_REGION:
        rgnRqst.friendlyName =
            "Get a specific region from the root";
        break;
      case CREATE_VM_ROOT:
        rgnRqst.friendlyName =
            "Create a new root VM region";
        break;
      case CREATE_VM_REGION:
        rgnRqst.friendlyName =
            "Create a new VM region";
        break;
      default:
        rgnRqst.friendlyName = String.format("Unknown operation %s",
            rgnRqst.action);
        break;
    }
  }
}
