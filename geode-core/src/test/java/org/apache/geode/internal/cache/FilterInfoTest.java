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
package org.apache.geode.internal.cache;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.FilterRoutingInfo.FilterInfo;
import org.apache.geode.internal.serialization.ByteArrayDataInput;

/**
 * Unit test for FilterRoutingInfo.FilterInfo
 */
public class FilterInfoTest {
  @Test
  public void validateSerialization() throws IOException, ClassNotFoundException {
    FilterInfo serialized = new FilterInfo();
    HashMap<Long, Integer> cqs = new HashMap<>();
    cqs.put(1L, 1);
    cqs.put(2L, 2);
    serialized.setCQs(cqs);
    Set<Long> clients = new HashSet<>();
    clients.add(1L);
    clients.add(2L);
    serialized.setInterestedClients(clients);
    Set<Long> clientsInv = new HashSet<>();
    clientsInv.add(3L);
    clientsInv.add(4L);
    serialized.setInterestedClientsInv(clientsInv);
    HeapDataOutputStream dataOut = new HeapDataOutputStream(Version.CURRENT);
    serialized.toData(dataOut);
    byte[] outputBytes = dataOut.toByteArray();
    FilterInfo deserialized = new FilterInfo();
    ByteArrayDataInput dataInput = new ByteArrayDataInput();
    dataInput.initialize(outputBytes, Version.CURRENT_ORDINAL);
    deserialized.fromData(dataInput);
    assertThat(deserialized.getCQs()).isEqualTo(cqs);
    assertThat(deserialized.getInterestedClients()).isEqualTo(clients);
    assertThat(deserialized.getInterestedClientsInv()).isEqualTo(clientsInv);
  }
}
