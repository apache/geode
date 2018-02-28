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
package org.apache.geode.internal.protocol.operations;

import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.protobuf.v1.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;
import org.apache.geode.internal.protocol.protobuf.v1.state.exception.ConnectionStateException;

/**
 * This interface is implemented by a object capable of handling request types 'Req' and returning a
 * response of type 'Resp'.
 *
 * The Serializer deserializes and serializes values in 'Req' and 'Resp'.
 *
 */
public interface ProtobufOperationHandler<Req, Resp> {
  /**
   * Decode the message, deserialize contained values using the serialization service, do the work
   * indicated on the provided cache, and return a response.
   *
   * @throws ConnectionStateException if the connection is in an invalid state for the operation in
   *         question.
   */
  Result<Resp> process(ProtobufSerializationService serializationService, Req request,
      MessageExecutionContext messageExecutionContext) throws InvalidExecutionContextException,
      ConnectionStateException, EncodingException, DecodingException;
}
