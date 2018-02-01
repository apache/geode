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

  @Override
  public V get(K key) throws IOException {
    final OutputStream outputStream = socket.getOutputStream();
    ClientProtocol.Message.newBuilder()
        .setRequest(ClientProtocol.Request.newBuilder().setGetRequest(RegionAPI.GetRequest
            .newBuilder().setRegionName(name).setKey(ValueEncoder.encodeValue(key))))
        .build().writeDelimitedTo(outputStream);

    final InputStream inputStream = socket.getInputStream();
    return (V) ValueEncoder.decodeValue(ClientProtocol.Message.parseDelimitedFrom(inputStream)
        .getResponse().getGetResponse().getResult());
  }

  @Override
  public RegionAttributes getRegionAttributes() throws IOException {
    final OutputStream outputStream = socket.getOutputStream();
    ClientProtocol.Message.newBuilder()
        .setRequest(ClientProtocol.Request.newBuilder()
            .setGetRegionRequest(RegionAPI.GetRegionRequest.newBuilder().setRegionName(name)))
        .build().writeDelimitedTo(outputStream);

    final InputStream inputStream = socket.getInputStream();
    return new RegionAttributes(ClientProtocol.Message.parseDelimitedFrom(inputStream).getResponse()
        .getGetRegionResponse().getRegion());
  }


  @Override
  public Map<K, V> getAll(Collection<K> keys) throws IOException {
    Map<K, V> values = new HashMap<>();

    final OutputStream outputStream = socket.getOutputStream();
    RegionAPI.GetAllRequest.Builder getAllRequest = RegionAPI.GetAllRequest.newBuilder();
    getAllRequest.setRegionName(name);
    for (K key : keys) {
      getAllRequest.addKey(ValueEncoder.encodeValue(key.toString()));
    }
    ClientProtocol.Message.newBuilder()
        .setRequest(ClientProtocol.Request.newBuilder().setGetAllRequest(getAllRequest)).build()
        .writeDelimitedTo(outputStream);

    final InputStream inputStream = socket.getInputStream();
    final RegionAPI.GetAllResponse getAllResponse =
        ClientProtocol.Message.parseDelimitedFrom(inputStream).getResponse().getGetAllResponse();
    for (BasicTypes.Entry entry : getAllResponse.getEntriesList()) {
      values.put((K) ValueEncoder.decodeValue(entry.getKey()),
          (V) ValueEncoder.decodeValue(entry.getValue()));
    }

    return values;
  }

  @Override
  public void put(K key, V value) throws IOException {
    final OutputStream outputStream = socket.getOutputStream();
    ClientProtocol.Message.newBuilder()
        .setRequest(ClientProtocol.Request.newBuilder()
            .setPutRequest(RegionAPI.PutRequest.newBuilder().setRegionName(name)
                .setEntry(ValueEncoder.encodeEntry(key, value))))
        .build().writeDelimitedTo(outputStream);

    final InputStream inputStream = socket.getInputStream();
    ClientProtocol.Message.parseDelimitedFrom(inputStream).getResponse().getPutResponse();
  }

  @Override
  public void putAll(Map<K, V> values) throws IOException {
    final OutputStream outputStream = socket.getOutputStream();
    RegionAPI.PutAllRequest.Builder putAllRequest = RegionAPI.PutAllRequest.newBuilder();
    putAllRequest.setRegionName(name);
    for (K key : values.keySet()) {
      putAllRequest.addEntry(ValueEncoder.encodeEntry(key, values.get(key)));
    }
    ClientProtocol.Message.newBuilder()
        .setRequest(ClientProtocol.Request.newBuilder().setPutAllRequest(putAllRequest)).build()
        .writeDelimitedTo(outputStream);

    final InputStream inputStream = socket.getInputStream();
    final RegionAPI.PutAllResponse putAllResponse =
        ClientProtocol.Message.parseDelimitedFrom(inputStream).getResponse().getPutAllResponse();
    if (0 < putAllResponse.getFailedKeysCount()) {
      StringBuilder builder = new StringBuilder();
      for (BasicTypes.KeyedError keyedError : putAllResponse.getFailedKeysList()) {
        if (0 < builder.length()) {
          builder.append(", ");
        }
        builder.append(ValueEncoder.decodeValue(keyedError.getKey()).toString());
      }
      throw new IOException("Unable to put the following keys: " + builder.toString());
    }
  }


  @Override
  public void remove(K key) throws IOException {
    final OutputStream outputStream = socket.getOutputStream();
    ClientProtocol.Message.newBuilder()
        .setRequest(ClientProtocol.Request.newBuilder().setRemoveRequest(RegionAPI.RemoveRequest
            .newBuilder().setRegionName(name).setKey(ValueEncoder.encodeValue(key))))
        .build().writeDelimitedTo(outputStream);

    final InputStream inputStream = socket.getInputStream();
    ClientProtocol.Message.parseDelimitedFrom(inputStream).getResponse().getRemoveResponse();
  }
}
