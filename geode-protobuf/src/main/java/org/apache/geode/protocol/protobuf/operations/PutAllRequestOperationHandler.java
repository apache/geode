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
package org.apache.geode.protocol.protobuf.operations;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.protocol.operations.OperationHandler;
import org.apache.geode.protocol.protobuf.BasicTypes;
import org.apache.geode.protocol.protobuf.ClientProtocol;
import org.apache.geode.protocol.protobuf.RegionAPI;
import org.apache.geode.protocol.protobuf.utilities.ProtobufResponseUtilities;
import org.apache.geode.protocol.protobuf.utilities.ProtobufUtilities;
import org.apache.geode.serialization.SerializationService;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class PutAllRequestOperationHandler
    implements OperationHandler<ClientProtocol.Request, ClientProtocol.Response> {
  private static Logger logger = LogManager.getLogger();

  private RegionAPI.PutAllRequest putAllRequest = null;
  private Region region = null;
  private Map<Object, Object> entries = null;

  @Override
  public ClientProtocol.Response process(SerializationService serializationService,
      ClientProtocol.Request request, Cache cache) {
    ClientProtocol.Response errorResponse = validatePutAllRequest(request);
    if (errorResponse == null) {
      errorResponse = determinePutAllRegion(cache);
    }
    if (errorResponse == null) {
      errorResponse = extractPutAllEntries(serializationService);
    }
    if (errorResponse == null) {
      try {
        region.putAll(entries);
      } catch (Exception ex) {
        return ProtobufResponseUtilities.createAndLogErrorResponse(ex.getMessage(), logger, ex);
      }

      return ProtobufResponseUtilities.createPutAllResponse();
    } else {
      return errorResponse;
    }
  }

  private ClientProtocol.Response validatePutAllRequest(ClientProtocol.Request request) {
    if (request.getRequestAPICase() != ClientProtocol.Request.RequestAPICase.PUTALLREQUEST) {
      return ProtobufResponseUtilities
          .createAndLogErrorResponse("Improperly formatted put request message.", logger, null);
    }

    putAllRequest = request.getPutAllRequest();
    return null;
  }

  private ClientProtocol.Response determinePutAllRegion(Cache cache) {
    String regionName = putAllRequest.getRegionName();
    region = cache.getRegion(regionName);

    if (region == null) {
      return ProtobufResponseUtilities.createAndLogErrorResponse(
          "Region passed by client did not exist: " + regionName, logger, null);
    } else {
      return null;
    }
  }

  // Read all of the entries out of the protobuf and return an error (without performing any puts)
  // if any of the entries can't be decoded
  private ClientProtocol.Response extractPutAllEntries(SerializationService serializationService) {
    entries = new HashMap();
    try {
      for (BasicTypes.Entry entry : putAllRequest.getEntryList()) {
        Object decodedValue = ProtobufUtilities.decodeValue(serializationService, entry.getValue());
        Object decodedKey = ProtobufUtilities.decodeValue(serializationService, entry.getKey());

        entries.put(decodedKey, decodedValue);
      }
    } catch (UnsupportedEncodingTypeException ex) {
      return ProtobufResponseUtilities.createAndLogErrorResponse("Encoding not supported ", logger,
          ex);
    } catch (CodecNotRegisteredForTypeException ex) {
      return ProtobufResponseUtilities
          .createAndLogErrorResponse("Codec error in protobuf deserialization ", logger, ex);
    }

    return null;
  }

}
