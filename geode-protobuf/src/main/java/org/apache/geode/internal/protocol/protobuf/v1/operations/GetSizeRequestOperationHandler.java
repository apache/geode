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
package org.apache.geode.internal.protocol.protobuf.v1.operations;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;

@Experimental
public class GetSizeRequestOperationHandler extends
    AbstractRegionRequestOperationHandler<RegionAPI.GetSizeRequest, RegionAPI.GetSizeResponse> {
  private static final Logger logger = LogService.getLogger();

  @Override
  protected Result<RegionAPI.GetSizeResponse> doOp(
      ProtobufSerializationService serializationService, RegionAPI.GetSizeRequest request,
      Region<Object, Object> region) {
    return Success.of(RegionAPI.GetSizeResponse.newBuilder().setSize(region.size()).build());
  }

  @Override
  protected String getRegionName(RegionAPI.GetSizeRequest request) {
    return request.getRegionName();
  }
}
