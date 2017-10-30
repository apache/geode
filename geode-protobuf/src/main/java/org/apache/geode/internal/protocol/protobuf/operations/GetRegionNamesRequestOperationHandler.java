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
package org.apache.geode.internal.protocol.protobuf.operations;

import java.util.Set;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.protocol.MessageExecutionContext;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.operations.OperationHandler;
import org.apache.geode.internal.protocol.protobuf.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.RegionAPI;
import org.apache.geode.internal.protocol.Result;
import org.apache.geode.internal.protocol.Success;
import org.apache.geode.internal.protocol.protobuf.utilities.ProtobufResponseUtilities;
import org.apache.geode.internal.protocol.serialization.SerializationService;

@Experimental
public class GetRegionNamesRequestOperationHandler implements
    OperationHandler<RegionAPI.GetRegionNamesRequest, RegionAPI.GetRegionNamesResponse, ClientProtocol.ErrorResponse> {

  @Override
  public Result<RegionAPI.GetRegionNamesResponse, ClientProtocol.ErrorResponse> process(
      SerializationService serializationService, RegionAPI.GetRegionNamesRequest request,
      MessageExecutionContext messageExecutionContext) throws InvalidExecutionContextException {
    Set<Region<?, ?>> regions = messageExecutionContext.getCache().rootRegions();
    return Success.of(ProtobufResponseUtilities.createGetRegionNamesResponse(regions));
  }
}
