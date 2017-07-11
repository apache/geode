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
package org.apache.geode.protocol.protobuf.utilities;

import org.apache.geode.protocol.protobuf.*;

/**
 * This class contains helper functions for generating ClientProtocol.Request objects
 *
 * Response building helpers can be found in {@link ProtobufResponseUtilities}, while more general
 * purpose helpers can be found in {@link ProtobufUtilities}
 */
public abstract class ProtobufRequestUtilities {
  /**
   * Creates a request object containing a RegionAPI.GetRequest
   *
   * @param regionName - Name of the region being fetched from
   * @param key - Encoded key, see createEncodedValue in {@link ProtobufRequestUtilities}
   * @return Request object containing the passed params.
   */
  public static ClientProtocol.Request createGetRequest(String regionName,
      BasicTypes.EncodedValue key) {
    RegionAPI.GetRequest getRequest =
        RegionAPI.GetRequest.newBuilder().setRegionName(regionName).setKey(key).build();
    return ClientProtocol.Request.newBuilder().setGetRequest(getRequest).build();
  }

  /**
   * Creates a request object containing a RegionAPI.RemoveRequest
   *
   * @param regionName - Name of the region being deleted from
   * @param key - Encoded key, see createEncodedValue in {@link ProtobufRequestUtilities}
   * @return Request object containing the passed params.
   */
  public static ClientProtocol.Request createRemoveRequest(String regionName,
      BasicTypes.EncodedValue key) {
    RegionAPI.RemoveRequest removeRequest =
        RegionAPI.RemoveRequest.newBuilder().setRegionName(regionName).setKey(key).build();
    return ClientProtocol.Request.newBuilder().setRemoveRequest(removeRequest).build();
  }

  /**
   * Creates a request object containing a RegionAPI.GetRegionNamesRequest
   *
   * @return Request object for a getRegionNames operation
   */
  public static ClientProtocol.Request createGetRegionNamesRequest() {
    return ClientProtocol.Request.newBuilder()
        .setGetRegionNamesRequest(RegionAPI.GetRegionNamesRequest.newBuilder()).build();
  }

  /**
   * Creates a request object containing a RegionAPI.PutRequest
   *
   * @param region - Name of the region to put data in
   * @param entry - Encoded key,value pair, see createEntry in {@link ProtobufRequestUtilities}
   * @return Request object containing the passed params.
   */
  public static ClientProtocol.Request createPutRequest(String region, BasicTypes.Entry entry) {
    RegionAPI.PutRequest putRequest =
        RegionAPI.PutRequest.newBuilder().setRegionName(region).setEntry(entry).build();
    return ClientProtocol.Request.newBuilder().setPutRequest(putRequest).build();
  }
}
