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
package org.apache.geode.internal.protocol.protobuf.utilities;

import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.protocol.protobuf.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.ProtocolErrorCode;


/**
 * This class contains helper functions for generating ClientProtocol.Response objects.
 * <p>
 * Request building helpers can be found in {@link ProtobufRequestUtilities}, while more general
 * purpose helpers can be found in {@link ProtobufUtilities}
 */
@Experimental
public abstract class ProtobufResponseUtilities {

  /**
   * This creates response object containing a ClientProtocol.ErrorResponse, and also logs the
   * passed error message and exception (if present) to the provided logger.
   *
   * @param errorMessage - description of the error
   * @param logger - logger to write the error message to
   * @param ex - exception which should be logged
   * @return An error response containing the first three parameters.
   */
  public static ClientProtocol.ErrorResponse createAndLogErrorResponse(ProtocolErrorCode errorCode,
      String errorMessage, Logger logger, Exception ex) {
    if (ex != null) {
      logger.error(errorMessage, ex);
    } else {
      logger.error(errorMessage);
    }
    return makeErrorResponse(errorCode.codeValue, errorMessage);
  }

  /**
   * This creates a response object containing a RegionAPI.GetRegionNamesResponse
   *
   * @param regionSet - A set of regions
   * @return A response object containing the names of the regions in the passed regionSet
   */
  public static RegionAPI.GetRegionNamesResponse createGetRegionNamesResponse(
      Set<Region<?, ?>> regionSet) {
    RegionAPI.GetRegionNamesResponse.Builder builder =
        RegionAPI.GetRegionNamesResponse.newBuilder();
    for (Region region : regionSet) {
      builder.addRegions(region.getName());
    }
    return builder.build();
  }

  public static ClientProtocol.ErrorResponse makeErrorResponse(int errorCode, String message) {
    return ClientProtocol.ErrorResponse.newBuilder()
        .setError(BasicTypes.Error.newBuilder().setErrorCode(errorCode).setMessage(message))
        .build();
  }
}
