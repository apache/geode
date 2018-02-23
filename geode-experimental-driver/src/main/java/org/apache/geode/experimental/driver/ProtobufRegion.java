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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;

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
  final String name;

  /**
   * Socket to a GemFire server that has Protobuf enabled.
   */
  final Socket socket;

  /**
   * Creates a region implementation for the region <code>name</code> that communicates via
   * <code>socket</code> to a GemFire server.
   *
   * @param name String that uniquely identifies the region.
   * @param socket Socket to a GemFire server that has Protobuf enabled.
   */
  ProtobufRegion(String name, Socket socket) {
    this.name = name;
    this.socket = socket;
  }

  private ClientProtocol.Message readResponse() throws IOException {
    final InputStream inputStream = socket.getInputStream();
    ClientProtocol.Message response = ClientProtocol.Message.parseDelimitedFrom(inputStream);
    final ClientProtocol.ErrorResponse errorResponse = response.getErrorResponse();
    if (errorResponse != null && errorResponse.hasError()) {
      throw new IOException(errorResponse.getError().getMessage());
    }
    return response;
  }


  @Override
  public RegionAttributes getRegionAttributes() throws IOException {
    final OutputStream outputStream = socket.getOutputStream();
    ClientProtocol.Message.newBuilder()
        .setGetRegionRequest(RegionAPI.GetRegionRequest.newBuilder().setRegionName(name)).build()
        .writeDelimitedTo(outputStream);

    return new RegionAttributes(readResponse().getGetRegionResponse().getRegion());
  }


  @Override
  public V get(K key) throws IOException {
    final OutputStream outputStream = socket.getOutputStream();
    ClientProtocol.Message.newBuilder().setGetRequest(
        RegionAPI.GetRequest.newBuilder().setRegionName(name).setKey(ValueEncoder.encodeValue(key)))
        .build().writeDelimitedTo(outputStream);

    final ClientProtocol.Message response = readResponse();
    return (V) ValueEncoder.decodeValue(response.getGetResponse().getResult());
  }

  @Override
  public Map<K, V> getAll(Collection<K> keys) throws IOException {
    Map<K, V> values = new HashMap<>();

    final OutputStream outputStream = socket.getOutputStream();
    RegionAPI.GetAllRequest.Builder getAllRequest = RegionAPI.GetAllRequest.newBuilder();
    getAllRequest.setRegionName(name);
    for (K key : keys) {
      getAllRequest.addKey(ValueEncoder.encodeValue(key));
    }
    ClientProtocol.Message.newBuilder().setGetAllRequest(getAllRequest).build()
        .writeDelimitedTo(outputStream);

    final RegionAPI.GetAllResponse getAllResponse = readResponse().getGetAllResponse();
    Map<Object, String> failures = new HashMap<>();
    if (getAllResponse.getFailuresCount() > 0) {
      for (BasicTypes.KeyedError keyedError : getAllResponse.getFailuresList()) {
        failures.put(ValueEncoder.decodeValue(keyedError.getKey()),
            keyedError.getError().getMessage());
      }
      throw new IOException("Unable to process the following keys: " + failures);
    }
    for (BasicTypes.Entry entry : getAllResponse.getEntriesList()) {
      values.put((K) ValueEncoder.decodeValue(entry.getKey()),
          (V) ValueEncoder.decodeValue(entry.getValue()));
    }

    return values;
  }

  @Override
  public void put(K key, V value) throws IOException {
    final OutputStream outputStream = socket.getOutputStream();
    ClientProtocol.Message.newBuilder().setPutRequest(RegionAPI.PutRequest.newBuilder()
        .setRegionName(name).setEntry(ValueEncoder.encodeEntry(key, value))).build()
        .writeDelimitedTo(outputStream);

    readResponse();
  }

  @Override
  public void putAll(Map<K, V> values) throws IOException {
    final OutputStream outputStream = socket.getOutputStream();
    RegionAPI.PutAllRequest.Builder putAllRequest = RegionAPI.PutAllRequest.newBuilder();
    putAllRequest.setRegionName(name);
    for (K key : values.keySet()) {
      putAllRequest.addEntry(ValueEncoder.encodeEntry(key, values.get(key)));
    }
    ClientProtocol.Message.newBuilder().setPutAllRequest(putAllRequest).build()
        .writeDelimitedTo(outputStream);

    final RegionAPI.PutAllResponse putAllResponse = readResponse().getPutAllResponse();
    if (0 < putAllResponse.getFailedKeysCount()) {
      Map<Object, String> failures = new HashMap<>();
      for (BasicTypes.KeyedError keyedError : putAllResponse.getFailedKeysList()) {
        failures.put(ValueEncoder.decodeValue(keyedError.getKey()),
            keyedError.getError().getMessage());
      }
      throw new IOException("Unable to put the following keys: " + failures);
    }
  }


  @Override
  public void remove(K key) throws IOException {
    final OutputStream outputStream = socket.getOutputStream();
    ClientProtocol.Message.newBuilder().setRemoveRequest(RegionAPI.RemoveRequest.newBuilder()
        .setRegionName(name).setKey(ValueEncoder.encodeValue(key))).build()
        .writeDelimitedTo(outputStream);

    readResponse();
  }
}
