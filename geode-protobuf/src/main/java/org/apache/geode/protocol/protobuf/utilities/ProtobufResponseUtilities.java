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

import org.apache.geode.cache.Region;
import org.apache.geode.protocol.protobuf.BasicTypes;
import org.apache.geode.protocol.protobuf.ClientProtocol;
import org.apache.geode.protocol.protobuf.RegionAPI;
import org.apache.logging.log4j.Logger;

import java.util.Set;

/**
 * This class contains helper functions for generating ClientProtocol.Response objects.
 * <p>
 * Request building helpers can be found in {@link ProtobufRequestUtilities}, while more general
 * purpose helpers can be found in {@link ProtobufUtilities}
 */
public abstract class ProtobufResponseUtilities {
  /**
   * This creates response object containing a ClientProtocol.ErrorResponse
   *
   * @param errorMessage - description of the error
   * @return An error response containing the above parameters
   */
  public static ClientProtocol.Response createErrorResponse(String errorMessage) {
    ClientProtocol.ErrorResponse error =
        ClientProtocol.ErrorResponse.newBuilder().setMessage(errorMessage).build();
    return ClientProtocol.Response.newBuilder().setErrorResponse(error).build();
  }

  /**
   * This creates response object containing a ClientProtocol.ErrorResponse, and also logs the
   * passed error message and exception (if present) to the provided logger.
   *
   * @param errorMessage - description of the error
   * @param logger - logger to write the error message to
   * @param ex - exception which should be logged
   * @return An error response containing the first three parameters.
   */
  public static ClientProtocol.Response createAndLogErrorResponse(String errorMessage,
      Logger logger, Exception ex) {
    if (ex != null) {
      logger.error(errorMessage, ex);
    } else {
      logger.error(errorMessage);
    }
    return createErrorResponse(errorMessage);
  }

  /**
   * This creates a response object containing a RegionAPI.GetResponse
   *
   * @param resultValue - the encoded result value, see createEncodedValue in
   *        {@link ProtobufUtilities}
   * @return A response indicating the passed value was found for a incoming GetRequest
   */
  public static ClientProtocol.Response createGetResponse(BasicTypes.EncodedValue resultValue) {
    RegionAPI.GetResponse getResponse =
        RegionAPI.GetResponse.newBuilder().setResult(resultValue).build();
    return ClientProtocol.Response.newBuilder().setGetResponse(getResponse).build();
  }

  /**
   * This creates a response object containing a RegionAPI.RemoveResponse
   *
   * @return A response indicating the entry with the passed key was removed
   */
  public static ClientProtocol.Response createRemoveResponse() {
    RegionAPI.RemoveResponse removeResponse = RegionAPI.RemoveResponse.newBuilder().build();
    return ClientProtocol.Response.newBuilder().setRemoveResponse(removeResponse).build();
  }

  /**
   * This creates a response object containing a RegionAPI.GetResponse
   *
   * @return A response indicating a failure to find a requested key or value
   */
  public static ClientProtocol.Response createNullGetResponse() {
    return ClientProtocol.Response.newBuilder().setGetResponse(RegionAPI.GetResponse.newBuilder())
        .build();
  }

  /**
   * This creates a response object containing a RegionAPI.GetRegionNamesResponse
   *
   * @param regionSet - A set of regions
   * @return A response object containing the names of the regions in the passed regionSet
   */
  public static ClientProtocol.Response createGetRegionNamesResponse(Set<Region<?, ?>> regionSet) {
    RegionAPI.GetRegionNamesResponse.Builder builder =
        RegionAPI.GetRegionNamesResponse.newBuilder();
    for (Region region : regionSet) {
      builder.addRegions(region.getName());
    }
    return ClientProtocol.Response.newBuilder().setGetRegionNamesResponse(builder).build();
  }

  /**
   * This creates a response object containing a RegionAPI.PutResponse
   *
   * @return A response object indicating a successful put
   */
  public static ClientProtocol.Response createPutResponse() {
    return ClientProtocol.Response.newBuilder().setPutResponse(RegionAPI.PutResponse.newBuilder())
        .build();
  }

  /**
   * This creates a response object containing a RegionAPI.GetAllResponse
   *
   * @param entries - key, value pairs for which lookups succeeded
   * @return A response object containing all the passed results
   */
  public static ClientProtocol.Response createGetAllResponse(Set<BasicTypes.Entry> entries) {
    RegionAPI.GetAllResponse.Builder builder = RegionAPI.GetAllResponse.newBuilder();
    builder.addAllEntries(entries);
    return ClientProtocol.Response.newBuilder().setGetAllResponse(builder).build();
  }

  /**
   * This creates a response for a putAll request
   *
   * @return A response object indicating any invalid keys (all others are assumed to have
   *         succeeded)
   */
  public static ClientProtocol.Response createPutAllResponse() {
    return ClientProtocol.Response.newBuilder()
        .setPutAllResponse(RegionAPI.PutAllResponse.newBuilder()).build();
  }
}
