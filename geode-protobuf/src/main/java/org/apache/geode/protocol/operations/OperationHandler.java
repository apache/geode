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
package org.apache.geode.protocol.operations;

import org.apache.geode.cache.Cache;
import org.apache.geode.protocol.protobuf.ProtobufOpsProcessor;
import org.apache.geode.protocol.protobuf.Result;
import org.apache.geode.serialization.SerializationService;

/**
 * This interface is implemented by a object capable of handling request types 'Req' and returning
 * an a response of type 'Resp'
 *
 * See {@link ProtobufOpsProcessor}
 */
public interface OperationHandler<Req, Resp> {
  /**
   * Decode the message, deserialize contained values using the serialization service, do the work
   * indicated on the provided cache, and return a response.
   */
  Result<Resp> process(SerializationService serializationService, Req request, Cache cache);
}

