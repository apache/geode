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
package org.apache.geode.internal.protocol.protobuf.v1.serialization;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;

/**
 * This interface converts a particular type to and from its binary representation.
 *
 * NOTE: it is expected that T will be one of the serialization types in @{@link SerializationType}.
 *
 * @param <T> the type this codec knows how to convert
 */
@Experimental
public interface TypeConverter<F, T> {
  T decode(F incoming) throws DecodingException;

  F encode(T incoming) throws EncodingException;

  /**
   * @return the SerializationType corresponding to T
   */
  SerializationType getSerializationType();
}
