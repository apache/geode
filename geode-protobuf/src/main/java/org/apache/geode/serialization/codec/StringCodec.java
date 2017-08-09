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
package org.apache.geode.serialization.codec;

import java.nio.charset.Charset;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.serialization.SerializationType;
import org.apache.geode.serialization.TypeCodec;

@Experimental
public class StringCodec implements TypeCodec<String> {
  private static final Charset UTF8 = Charset.forName("UTF-8");

  @Override
  public String decode(byte[] incoming) {
    return new String(incoming, UTF8);
  }

  @Override
  public byte[] encode(String incoming) {
    return incoming.getBytes(UTF8);
  }

  @Override
  public SerializationType getSerializationType() {
    return SerializationType.STRING;
  }
}
