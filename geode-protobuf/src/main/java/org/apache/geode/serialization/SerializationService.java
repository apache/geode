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
package org.apache.geode.serialization;

import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;

/**
 * This interface takes an protocol specific encodingTypeValue enum and converts between objects and
 * bytes using the encodingTypeValue to decide what encoding type to use.
 *
 * @param <T> the enumeration of types known to a particular protocol
 */
public interface SerializationService<T> {
  Object decode(T encodingTypeValue, byte[] value)
      throws UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException;

  byte[] encode(T encodingTypeValue, Object value)
      throws UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException;
}
