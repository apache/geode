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
package org.apache.geode.internal.protocol.protobuf.v1;

import java.util.Set;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.protocol.protobuf.v1.utilities.ProtobufUtilities;

/**
 * This class contains helper functions for generating ClientProtocol.Message objects
 *
 * More general purpose helpers can be found in {@link ProtobufUtilities}
 */
@Experimental
public abstract class ProtobufRequestUtilities {
  /**
   * Creates a request object containing a RegionAPI.GetRequest
   *
   * @param regionName - Name of the region being fetched from
   * @param key - Encoded key, see createEncodedValue in {@link ProtobufRequestUtilities}
   * @return Request object containing the passed params.
   */
  public static ClientProtocol.Message createGetRequest(String regionName,
      BasicTypes.EncodedValue key) {
    RegionAPI.GetRequest getRequest =
        RegionAPI.GetRequest.newBuilder().setRegionName(regionName).setKey(key).build();
    return ClientProtocol.Message.newBuilder().setGetRequest(getRequest).build();
  }

  /**
   * Creates a request object containing a RegionAPI.ClearRequest
   *
   * @param regionName - Name of the region being cleared
   * @return Request object containing the passed params.
   */
  public static ClientProtocol.Message createClearRequest(String regionName) {
    RegionAPI.ClearRequest clearRequest =
        RegionAPI.ClearRequest.newBuilder().setRegionName(regionName).build();
    return ClientProtocol.Message.newBuilder().setClearRequest(clearRequest).build();
  }

  /**
   * Creates a request object containing a RegionAPI.RemoveRequest
   *
   * @param regionName - Name of the region being deleted from
   * @param key - Encoded key, see createEncodedValue in {@link ProtobufRequestUtilities}
   * @return Request object containing the passed params.
   */
  public static ClientProtocol.Message createRemoveRequest(String regionName,
      BasicTypes.EncodedValue key) {
    RegionAPI.RemoveRequest removeRequest =
        RegionAPI.RemoveRequest.newBuilder().setRegionName(regionName).setKey(key).build();
    return ClientProtocol.Message.newBuilder().setRemoveRequest(removeRequest).build();
  }

  /**
   * Creates a request object containing a RegionAPI.GetRegionNamesRequest
   *
   * @return Request object for a getRegionNames operation
   */
  public static RegionAPI.GetRegionNamesRequest createGetRegionNamesRequest() {
    return RegionAPI.GetRegionNamesRequest.newBuilder().build();
  }

  /**
   * Creates a request object containing a RegionAPI.PutRequest
   *
   * @param region - Name of the region to put data in
   * @param entry - Encoded key,value pair, see createEntry in {@link ProtobufRequestUtilities}
   * @return Request object containing the passed params.
   */
  public static ClientProtocol.Message createPutRequest(String region, BasicTypes.Entry entry) {
    RegionAPI.PutRequest putRequest =
        RegionAPI.PutRequest.newBuilder().setRegionName(region).setEntry(entry).build();
    return ClientProtocol.Message.newBuilder().setPutRequest(putRequest).build();
  }

  /**
   * Creates a request object containing a RegionAPI.PutIfAbsentRequest
   *
   * @param region - Name of the region to put data in
   * @param entry - Encoded key,value pair, see createEntry in {@link ProtobufRequestUtilities}
   * @return Request object containing the passed params.
   */
  public static ClientProtocol.Message createPutIfAbsentRequest(String region,
      BasicTypes.Entry entry) {
    RegionAPI.PutIfAbsentRequest putIfAbsentRequest =
        RegionAPI.PutIfAbsentRequest.newBuilder().setRegionName(region).setEntry(entry).build();
    return ClientProtocol.Message.newBuilder().setPutIfAbsentRequest(putIfAbsentRequest).build();
  }

  /**
   * Create a request to get the values for multiple keys
   *
   * @param regionName - Name of the region to fetch from
   * @param keys - Set of keys being fetched
   * @return Request object containing the getAll request
   */
  public static RegionAPI.GetAllRequest createGetAllRequest(String regionName,
      Set<BasicTypes.EncodedValue> keys) {
    RegionAPI.GetAllRequest.Builder getAllRequestBuilder =
        RegionAPI.GetAllRequest.newBuilder().setRegionName(regionName);
    getAllRequestBuilder.addAllKey(keys);
    return getAllRequestBuilder.build();
  }

  /**
   * Create a request to insert multiple entries in a region
   *
   * @param regionName - Region to which entries are being added
   * @param entries - key, value pairs to add to the region
   * @return Request object containing the putAll request for the passed parameters
   */
  public static ClientProtocol.Message createPutAllRequest(String regionName,
      Set<BasicTypes.Entry> entries) {
    RegionAPI.PutAllRequest.Builder putAllRequestBuilder =
        RegionAPI.PutAllRequest.newBuilder().setRegionName(regionName);
    putAllRequestBuilder.addAllEntry(entries);
    return ClientProtocol.Message.newBuilder().setPutAllRequest(putAllRequestBuilder).build();
  }

  /**
   * Create a request to retrieve a server.
   *
   * @return Request object containing the get-server request.
   */
  public static LocatorAPI.GetServerRequest createGetServerRequest() {
    LocatorAPI.GetServerRequest.Builder builder = LocatorAPI.GetServerRequest.newBuilder();
    return builder.build();
  }

  /**
   * Create a request to retrieve a server from a server group.
   *
   * @param serverGroup Server group name.
   * @return Request object containing the get-server request.
   */
  public static LocatorAPI.GetServerRequest createGetServerRequest(String serverGroup) {
    LocatorAPI.GetServerRequest.Builder builder = LocatorAPI.GetServerRequest.newBuilder();
    builder.setServerGroup(serverGroup);
    return builder.build();
  }
}
