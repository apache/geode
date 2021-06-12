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
package org.apache.geode.internal.cache.tier.sockets;

import java.util.HashMap;
import java.util.Map;

import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqListener;

public class TestAutoSerializerCqListener implements CqListener {
  private int numEvents = 0;
  private int numErrors = 0;
  public Map<String, TestAutoSerializerObject1> testAutoSerializerObject1 = new HashMap<>();
  public Map<String, TestAutoSerializerObject2> testAutoSerializerObject2 = new HashMap<>();

  public int getNumEvents() {
    return numEvents;
  }

  public int getNumErrors() {
    return numErrors;
  }

  @Override
  public void onEvent(CqEvent aCqEvent) {
    Object obj = aCqEvent.getNewValue();
    if (obj instanceof TestAutoSerializerObject1) {
      testAutoSerializerObject1.put((String) aCqEvent.getKey(),
          (TestAutoSerializerObject1) aCqEvent.getNewValue());
    } else if (obj instanceof TestAutoSerializerObject2) {
      testAutoSerializerObject2.put((String) aCqEvent.getKey(),
          (TestAutoSerializerObject2) aCqEvent.getNewValue());
    }
    numEvents++;
  }

  @Override
  public void onError(CqEvent aCqEvent) {
    numErrors++;
  }

  @Override
  public void close() {}
}
