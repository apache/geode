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
package org.apache.geode.cache.client.internal.locator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.BucketServerLocation66;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

public class SerializationHelper {

  private static void writeServerLocations(Collection<ServerLocation> serverLocations,
      DataOutput out, SerializationContext context) throws IOException {
    if (serverLocations == null) {
      out.writeInt(-1);
      return;
    }
    out.writeInt(serverLocations.size());
    for (ServerLocation serverLocation : serverLocations) {
      context.getSerializer().invokeToData(serverLocation, out);
    }
  }

  private static void writeBucketServerLocations(
      Collection<BucketServerLocation66> bucketServerLocations, DataOutput out,
      SerializationContext context) throws IOException {
    if (bucketServerLocations == null) {
      out.writeInt(-1);
      return;
    }
    out.writeInt(bucketServerLocations.size());
    for (BucketServerLocation66 serverLocation : bucketServerLocations) {
      context.getSerializer().invokeToData(serverLocation, out);
    }
  }

  private static <T extends Collection<ServerLocation>> T readServerLocations(DataInput in,
      int size, T serverLocations, DeserializationContext context)
      throws IOException, ClassNotFoundException {

    for (int i = 0; i < size; i++) {
      ServerLocation next = new ServerLocation();
      context.getDeserializer().invokeFromData(next, in);
      serverLocations.add(next);
    }
    return serverLocations;
  }


  static ArrayList<ServerLocation> readServerLocationList(DataInput in,
      DeserializationContext context)
      throws IOException, ClassNotFoundException {
    int size = in.readInt();
    if (size < 0) {
      return null;
    }
    return readServerLocations(in, size, new ArrayList<>(size), context);
  }

  static void writeServerLocationList(List<ServerLocation> serverLocations,
      DataOutput out, SerializationContext context) throws IOException {
    writeServerLocations(serverLocations, out, context);
  }

  static void writeServerLocationSet(Set<ServerLocation> serverLocations,
      DataOutput out, SerializationContext context) throws IOException {
    writeServerLocations(serverLocations, out, context);
  }

  public static void writeBucketServerLocationSet(Set<BucketServerLocation66> bucketServerLocations,
      DataOutput out, SerializationContext context) throws IOException {
    writeBucketServerLocations(bucketServerLocations, out, context);
  }

  static HashSet<ServerLocation> readServerLocationSet(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    int size = in.readInt();
    if (size < 0) {
      return null;
    }
    return readServerLocations(in, size, new HashSet<>(size), context);
  }

  public static HashSet<BucketServerLocation66> readBucketServerLocationSet(DataInput in,
      DeserializationContext context)
      throws IOException, ClassNotFoundException {
    int size = in.readInt();
    if (size < 0) {
      return null;
    }
    HashSet<BucketServerLocation66> bucketServerLocations = new HashSet<>(size);
    for (int i = 0; i < size; i++) {
      BucketServerLocation66 next = new BucketServerLocation66();
      context.getDeserializer().invokeFromData(next, in);
      bucketServerLocations.add(next);
    }
    return bucketServerLocations;
  }


  private SerializationHelper() {}

}
