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

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.admin.CacheInfo;
import org.apache.geode.internal.i18n.LocalizedStrings;

import java.io.*;

/**
 * A message that is sent to a VM that hosts a cache to perform an
 * administrative operation on one of its bridge servers.
 *
 * @since GemFire 4.0
 */
public final class BridgeServerRequest extends AdminRequest {

  /** Add a new bridge server */
  static final int ADD_OPERATION = 10;

  /** Get info about a bridge server */
  static final int INFO_OPERATION = 11;

  /** Start a bridge server */
  static final int START_OPERATION = 12;

  /** Stop a bridge server */
  static final int STOP_OPERATION = 13;

  ///////////////////  Instance Fields  ////////////////////

  /** The id of the cache in which the bridge server resides */
  private int cacheId;

  /** The type of operation to perform */
  private int operation;

  /** Bridge server configuration info for performing an operation */
  private RemoteBridgeServer bridgeInfo;

  /** The id of bridge server to get information about */
  private int bridgeId;

  ////////////////////  Static Methods  ////////////////////

  /**
   * Creates a <code>BridgeServerRequest</code> for adding a new
   * bridge server.
   */
  public static BridgeServerRequest createForAdd(CacheInfo cache) {
    BridgeServerRequest request = new BridgeServerRequest();
    request.cacheId = cache.getId();
    request.operation = ADD_OPERATION;
    request.friendlyName = LocalizedStrings.BridgeServerRequest_ADD_BRIDGE_SERVER.toLocalizedString();
    request.bridgeInfo = null;
    return request;
  }

  /**
   * Creates a <code>BridgeServerRequest</code> for adding a new
   * bridge server.
   */
  public static BridgeServerRequest createForInfo(CacheInfo cache,
                                                  int id) {
    BridgeServerRequest request = new BridgeServerRequest();
    request.cacheId = cache.getId();
    request.operation = INFO_OPERATION;
    request.friendlyName = LocalizedStrings.BridgeServerRequest_GET_INFO_ABOUT_BRIDGE_SERVER_0.toLocalizedString(Integer.valueOf(id));
    request.bridgeId = id;
    request.bridgeInfo = null;
    return request;
  }

  /**
   * Creates a <code>BridgeServerRequest</code> for starting a
   * bridge server.
   */
  public static BridgeServerRequest createForStart(CacheInfo cache,
                                                   RemoteBridgeServer bridge) {
    BridgeServerRequest request = new BridgeServerRequest();
    request.cacheId = cache.getId();
    request.operation = START_OPERATION;
    request.friendlyName = LocalizedStrings.BridgeServerRequest_START_BRIDGE_SERVER_0.toLocalizedString(bridge);
    request.bridgeInfo = bridge;
    return request;
  }

  /**
   * Creates a <code>BridgeServerRequest</code> for stopping a
   * bridge server.
   */
  public static BridgeServerRequest createForStop(CacheInfo cache,
                                                  RemoteBridgeServer bridge) {
    BridgeServerRequest request = new BridgeServerRequest();
    request.cacheId = cache.getId();
    request.operation = STOP_OPERATION;
    request.friendlyName = LocalizedStrings.BridgeServerRequest_STOP_BRIDGE_SERVER_0.toLocalizedString(bridge);
    request.bridgeInfo = bridge;
    return request;
  }

  /**
   * Returns a description of the given operation
   */
  private static String getOperationDescription(int op) {
    switch (op) {
    case ADD_OPERATION:
      return LocalizedStrings.BridgeServerRequest_ADD_BRIDGE_SERVER.toLocalizedString();
    case INFO_OPERATION:
      return LocalizedStrings.BridgeServerRequest_GET_INFO_ABOUT_BRIDGE_SERVER_0.toLocalizedString();
    default:
      return LocalizedStrings.BridgeServerRequest_UNKNOWN_OPERATION_0.toLocalizedString(Integer.valueOf(op));
    }
  }

  ////////////////////  Instance Methods  ////////////////////

  /**
   * Creates a <Code>BridgeServerResponse</code> to this request
   */
  @Override  
  protected AdminResponse createResponse(DistributionManager dm) {
    return BridgeServerResponse.create(dm, this);
  }

  /**
   * Returns the id of the cache in which the bridge server resides
   */
  int getCacheId() {
    return this.cacheId;
  }

  /**
   * Returns this operation to be performed
   */
  int getOperation() {
    return this.operation;
  }

  /**
   * Returns the id of the bridge server for which information is
   * requested.
   */
  int getBridgeId() {
    return this.bridgeId;
  }

  /**
   * Returns the information about the bridge to operate on
   */
  RemoteBridgeServer getBridgeInfo() {
    return this.bridgeInfo;
  }

  public int getDSFID() {
    return BRIDGE_SERVER_REQUEST;
  }

  @Override  
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.cacheId);
    out.writeInt(this.operation);
    DataSerializer.writeObject(this.bridgeInfo, out);
    out.writeInt(this.bridgeId);
  }

  @Override  
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.cacheId = in.readInt();
    this.operation = in.readInt();
    this.bridgeInfo =
      (RemoteBridgeServer) DataSerializer.readObject(in);
    this.bridgeId = in.readInt();
  }

  @Override  
  public String toString() {
    return "BridgeServerRequest: " +
      getOperationDescription(this.operation);
  }

}
