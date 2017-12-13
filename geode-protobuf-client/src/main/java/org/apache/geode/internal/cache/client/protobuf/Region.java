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
package org.apache.geode.internal.cache.client.protobuf;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.ByteString;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;

@Experimental
public class Region<K, V> {
  final String name;
  final Socket socket;

  BasicTypes.EncodedValue encodeValue(Object unencodedValue) {
    BasicTypes.EncodedValue.Builder builder = BasicTypes.EncodedValue.newBuilder();
    if (Integer.class.equals(unencodedValue.getClass())) {
      builder.setIntResult((Integer) unencodedValue);
    } else if (Long.class.equals(unencodedValue.getClass())) {
      builder.setLongResult((Long) unencodedValue);
    } else if (Short.class.equals(unencodedValue.getClass())) {
      builder.setShortResult((Short) unencodedValue);
    } else if (Byte.class.equals(unencodedValue.getClass())) {
      builder.setByteResult((Byte) unencodedValue);
    } else if (Double.class.equals(unencodedValue.getClass())) {
      builder.setDoubleResult((Double) unencodedValue);
    } else if (Float.class.equals(unencodedValue.getClass())) {
      builder.setFloatResult((Float) unencodedValue);
    } else if (byte[].class.equals(unencodedValue.getClass())) {
      builder.setBinaryResult(ByteString.copyFrom((byte[]) unencodedValue));
    } else if (Boolean.class.equals(unencodedValue.getClass())) {
      builder.setBooleanResult((Boolean) unencodedValue);
    } else if (String.class.equals(unencodedValue.getClass())) {
      builder.setStringResult((String) unencodedValue);
    }
    return builder.build();
  }

  Object decodeValue(BasicTypes.EncodedValue encodedValue) {
    switch (encodedValue.getValueCase()) {
      case BINARYRESULT:
        return encodedValue.getBinaryResult().toByteArray();
      case BOOLEANRESULT:
        return encodedValue.getBooleanResult();
      case BYTERESULT:
        return (byte) encodedValue.getByteResult();
      case DOUBLERESULT:
        return encodedValue.getDoubleResult();
      case FLOATRESULT:
        return encodedValue.getFloatResult();
      case INTRESULT:
        return encodedValue.getIntResult();
      case LONGRESULT:
        return encodedValue.getLongResult();
      case SHORTRESULT:
        return (short) encodedValue.getShortResult();
      case STRINGRESULT:
        return encodedValue.getStringResult();
      default:
        return null;
    }
  }

  BasicTypes.Entry encodeEntry(Object unencodedKey, Object unencodedValue) {
    if (unencodedValue == null) {
      return BasicTypes.Entry.newBuilder().setKey(encodeValue(unencodedKey)).build();
    }
    return BasicTypes.Entry.newBuilder().setKey(encodeValue(unencodedKey))
        .setValue(encodeValue(unencodedValue)).build();
  }

  Region(String name, Socket socket) {
    this.name = name;
    this.socket = socket;
  }

  public V get(K key) throws Exception {
    final OutputStream outputStream = socket.getOutputStream();
    ClientProtocol.Message.newBuilder()
        .setRequest(ClientProtocol.Request.newBuilder().setGetRequest(RegionAPI.GetRequest
            .newBuilder().setRegionName(name).setKey(encodeValue(key.toString()))))
        .build().writeDelimitedTo(outputStream);

    // TODO: How does one get a java.lang.Object out of
    // org.apache.geode.internal.protocol.protobuf.v1.EncodedValue?
    final InputStream inputStream = socket.getInputStream();
    return (V) ClientProtocol.Message.parseDelimitedFrom(inputStream).getResponse().getGetResponse()
        .getResult().getStringResult();
  }

  public Map<K, V> getAll(Collection<K> keys) throws Exception {
    Map<K, V> values = new HashMap<>();

    final OutputStream outputStream = socket.getOutputStream();
    RegionAPI.GetAllRequest.Builder getAllRequest = RegionAPI.GetAllRequest.newBuilder();
    getAllRequest.setRegionName(name);
    for (K key : keys) {
      getAllRequest.addKey(encodeValue(key.toString()));
    }
    ClientProtocol.Message.newBuilder()
        .setRequest(ClientProtocol.Request.newBuilder().setGetAllRequest(getAllRequest)).build()
        .writeDelimitedTo(outputStream);

    final InputStream inputStream = socket.getInputStream();
    final RegionAPI.GetAllResponse getAllResponse =
        ClientProtocol.Message.parseDelimitedFrom(inputStream).getResponse().getGetAllResponse();
    for (BasicTypes.Entry entry : getAllResponse.getEntriesList()) {
      // TODO: How does one get a java.lang.Object out of
      // org.apache.geode.internal.protocol.protobuf.v1.EncodedValue?
      // TODO values.put((K) entry.getKey().getStringResult(), (V)
      // entry.getValue().getStringResult());
    }

    return values;
  }

  public void put(K key, V value) throws Exception {
    final OutputStream outputStream = socket.getOutputStream();
    ClientProtocol.Message.newBuilder()
        .setRequest(ClientProtocol.Request.newBuilder()
            .setPutRequest(RegionAPI.PutRequest.newBuilder().setRegionName(name)
                .setEntry(encodeEntry(key.toString(), value))))
        .build().writeDelimitedTo(outputStream);

    final InputStream inputStream = socket.getInputStream();
    ClientProtocol.Message.parseDelimitedFrom(inputStream).getResponse().getPutResponse();
  }

  public void putAll(Map<K, V> values) throws Exception {
    final OutputStream outputStream = socket.getOutputStream();
    RegionAPI.PutAllRequest.Builder putAllRequest = RegionAPI.PutAllRequest.newBuilder();
    putAllRequest.setRegionName(name);
    for (K key : values.keySet()) {
      putAllRequest.addEntry(encodeEntry(key, values.get(key)));
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
        builder.append(decodeValue(keyedError.getKey()).toString());
      }
      throw new Exception("Unable to put the following keys: " + builder.toString());
    }
  }

  public void remove(K key) throws Exception {
    final OutputStream outputStream = socket.getOutputStream();
    ClientProtocol.Message.newBuilder()
        .setRequest(ClientProtocol.Request.newBuilder().setRemoveRequest(RegionAPI.RemoveRequest
            .newBuilder().setRegionName(name).setKey(encodeValue(key.toString()))))
        .build().writeDelimitedTo(outputStream);

    final InputStream inputStream = socket.getInputStream();
    ClientProtocol.Message.parseDelimitedFrom(inputStream).getResponse().getRemoveResponse();
  }

  public void removeAll(Collection<K> keys) throws Exception {
    final OutputStream outputStream = socket.getOutputStream();
    RegionAPI.RemoveAllRequest.Builder removeAllRequest = RegionAPI.RemoveAllRequest.newBuilder();
    removeAllRequest.setRegionName(name);
    for (K key : keys) {
      removeAllRequest.addKey(encodeValue(key.toString()));
    }
    ClientProtocol.Message.newBuilder()
        .setRequest(ClientProtocol.Request.newBuilder().setRemoveAllRequest(removeAllRequest))
        .build().writeDelimitedTo(outputStream);

    final InputStream inputStream = socket.getInputStream();
    if (!ClientProtocol.Message.parseDelimitedFrom(inputStream).getResponse().getRemoveAllResponse()
        .getSuccess()) {
      throw new Exception("Unable to remove all entries");
    }
  }
}
