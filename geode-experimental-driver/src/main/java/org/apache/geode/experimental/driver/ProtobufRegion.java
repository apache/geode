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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol.Message;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol.Message.MessageTypeCase;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI.GetRequest;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI.PutRequest;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI.RemoveRequest;

/**
 * Implements the behaviors of a GemFire region. Send and receives Protobuf messages on the provided
 * socket to communicate with a GemFire server that has Protobuf enabled.
 *
 * <strong>This code is an experimental prototype and is presented "as is" with no warranty,
 * suitability, or fitness of purpose implied.</strong>
 *
 * @param <K> Type of region keys.
 * @param <V> Type of region values.
 */
@Experimental
public class ProtobufRegion<K, V> implements Region<K, V> {
  /**
   * String that uniquely identifies the region.
   */
  private final String name;

  private final ProtobufChannel protobufChannel;
  private final ValueEncoder valueEncoder;

  /**
   * Creates a region implementation for the region <code>name</code> that communicates via
   * <code>socket</code> to a GemFire server.
   *
   * @param name String that uniquely identifies the region.
   */
  ProtobufRegion(String name, ProtobufChannel channel, ValueEncoder valueEncoder) {
    this.name = name;
    protobufChannel = channel;
    this.valueEncoder = valueEncoder;
  }

  @Override
  public int size() throws IOException {
    final Message request = Message.newBuilder()
        .setGetSizeRequest(RegionAPI.GetSizeRequest.newBuilder().setRegionName(name)).build();
    return protobufChannel.sendRequest(request, MessageTypeCase.GETSIZERESPONSE)
        .getGetSizeResponse().getSize();
  }

  @Override
  public V get(K key) throws IOException {
    Message request = Message.newBuilder()
        .setGetRequest(
            GetRequest.newBuilder().setRegionName(name).setKey(valueEncoder.encodeValue(key)))
        .build();
    final Message response = protobufChannel.sendRequest(request, MessageTypeCase.GETRESPONSE);

    return valueEncoder.decodeValue(response.getGetResponse().getResult());
  }

  @Override
  public Map<K, V> getAll(Collection<K> keys) throws IOException {
    Map<K, V> values = new HashMap<>();

    RegionAPI.GetAllRequest.Builder getAllRequest = RegionAPI.GetAllRequest.newBuilder();
    getAllRequest.setRegionName(name);
    for (K key : keys) {
      getAllRequest.addKey(valueEncoder.encodeValue(key));
    }
    Message request = Message.newBuilder().setGetAllRequest(getAllRequest).build();

    Message message = protobufChannel.sendRequest(request, MessageTypeCase.GETALLRESPONSE);

    final RegionAPI.GetAllResponse getAllResponse = message.getGetAllResponse();
    Map<Object, String> failures = new HashMap<>();
    if (getAllResponse.getFailuresCount() > 0) {
      for (BasicTypes.KeyedError keyedError : getAllResponse.getFailuresList()) {
        failures.put(valueEncoder.decodeValue(keyedError.getKey()),
            keyedError.getError().getMessage());
      }
      throw new IOException("Unable to process the following keys: " + failures);
    }
    for (BasicTypes.Entry entry : getAllResponse.getEntriesList()) {
      values.put(valueEncoder.decodeValue(entry.getKey()),
          valueEncoder.decodeValue(entry.getValue()));
    }

    return values;
  }

  @Override
  public void put(K key, V value) throws IOException {
    final Message request = Message.newBuilder().setPutRequest(
        PutRequest.newBuilder().setRegionName(name).setEntry(valueEncoder.encodeEntry(key, value)))
        .build();

    protobufChannel.sendRequest(request, MessageTypeCase.PUTRESPONSE);
  }

  @Override
  public void putAll(Map<K, V> values) throws IOException {
    RegionAPI.PutAllRequest.Builder putAllRequest = RegionAPI.PutAllRequest.newBuilder();
    putAllRequest.setRegionName(name);
    for (K key : values.keySet()) {
      putAllRequest.addEntry(valueEncoder.encodeEntry(key, values.get(key)));
    }
    final Message request = Message.newBuilder().setPutAllRequest(putAllRequest).build();

    final RegionAPI.PutAllResponse putAllResponse =
        protobufChannel.sendRequest(request, MessageTypeCase.PUTALLRESPONSE).getPutAllResponse();
    if (0 < putAllResponse.getFailedKeysCount()) {
      Map<Object, String> failures = new HashMap<>();
      for (BasicTypes.KeyedError keyedError : putAllResponse.getFailedKeysList()) {
        failures.put(valueEncoder.decodeValue(keyedError.getKey()),
            keyedError.getError().getMessage());
      }
      throw new IOException("Unable to put the following keys: " + failures);
    }
  }

  @Override
  public void clear() throws IOException {
    final Message request = Message.newBuilder()
        .setClearRequest(RegionAPI.ClearRequest.newBuilder().setRegionName(name)).build();

    protobufChannel.sendRequest(request, MessageTypeCase.CLEARRESPONSE);
  }

  @Override
  public V putIfAbsent(K key, V value) throws IOException {
    final RegionAPI.PutIfAbsentRequest.Builder putIfAbsentRequest = RegionAPI.PutIfAbsentRequest
        .newBuilder().setRegionName(name).setEntry(valueEncoder.encodeEntry(key, value));

    final Message request = Message.newBuilder().setPutIfAbsentRequest(putIfAbsentRequest).build();

    final RegionAPI.PutIfAbsentResponse putIfAbsentResponse = protobufChannel
        .sendRequest(request, MessageTypeCase.PUTIFABSENTRESPONSE).getPutIfAbsentResponse();


    return valueEncoder.decodeValue(putIfAbsentResponse.getOldValue());
  }

  @Override
  public void remove(K key) throws IOException {
    final Message request = Message.newBuilder()
        .setRemoveRequest(
            RemoveRequest.newBuilder().setRegionName(name).setKey(valueEncoder.encodeValue(key)))
        .build();

    protobufChannel.sendRequest(request, MessageTypeCase.REMOVERESPONSE);
  }

  @Override
  public Set<K> keySet() throws IOException {
    final Message request = Message.newBuilder()
        .setKeySetRequest(RegionAPI.KeySetRequest.newBuilder().setRegionName(name)).build();
    final Message message = protobufChannel.sendRequest(request, MessageTypeCase.KEYSETRESPONSE);
    final RegionAPI.KeySetResponse keySetResponse = message.getKeySetResponse();

    Set<K> keys = new HashSet<>(keySetResponse.getKeysCount());
    for (BasicTypes.EncodedValue value : keySetResponse.getKeysList()) {
      keys.add(valueEncoder.decodeValue(value));
    }
    return keys;
  }
}
