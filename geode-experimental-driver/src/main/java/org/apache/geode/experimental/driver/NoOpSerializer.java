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
package org.apache.geode.experimental.driver;

import java.io.IOException;

import com.google.protobuf.ByteString;

public class NoOpSerializer implements ValueSerializer {
  @Override
  public ByteString serialize(Object object) throws IOException {
    return null;
  }

  @Override
  public Object deserialize(ByteString data) throws IOException, ClassNotFoundException {
    throw new IllegalStateException("No ValueSerializer is set to handle a custom value");
  }

  @Override
  public String getID() {
    return "";
  }

  @Override
  public boolean supportsPrimitives() {
    return false;
  }
}
