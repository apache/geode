/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.client.internal.locator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.BucketServerLocation66;

/**
 *
 */
public class SerializationHelper {

  private static void writeServerLocations(Collection/*<ServerLocation>*/ serverLocations, DataOutput out) throws IOException {
    if(serverLocations == null) {
      out.writeInt(-1);
      return;
    }
    int length = serverLocations.size();
    out.writeInt(length);
    for(Iterator itr = serverLocations.iterator(); itr.hasNext(); ) {
      ServerLocation next = (ServerLocation) itr.next();
      InternalDataSerializer.invokeToData(next, out);
    }
  }

  private static void writeBucketServerLocations(Collection<BucketServerLocation66> bucketServerLocations, DataOutput out) throws IOException {
    if(bucketServerLocations == null) {
      out.writeInt(-1);
      return;
    }
    int length = bucketServerLocations.size();
    out.writeInt(length);
    for(Iterator itr = bucketServerLocations.iterator(); itr.hasNext(); ) {
      ServerLocation next = (ServerLocation) itr.next();
      InternalDataSerializer.invokeToData(next, out);
    }
  }
  
  public static ArrayList/*<ServerLocation>*/ readServerLocationList(DataInput in) throws IOException, ClassNotFoundException {
    int size = in.readInt();
    if(size < 0) {
      return null;
    }
    ArrayList serverLocations  = new ArrayList(size);
    for(int i = 0; i < size; i++) {
      ServerLocation next = new ServerLocation();
      InternalDataSerializer.invokeFromData(next, in);
      serverLocations.add(next);
    }
    return serverLocations;
  }
  
  public static void writeServerLocationList(List/*<ServerLocation>*/ serverLocations, DataOutput out) throws IOException {
    writeServerLocations(serverLocations, out);
  }

  public static void writeServerLocationSet(Set/*<ServerLocation>*/ serverLocations, DataOutput out) throws IOException {
    writeServerLocations(serverLocations, out);
  }

  public static void writeBucketServerLocationSet(Set<BucketServerLocation66> bucketServerLocations, DataOutput out) throws IOException {
    writeBucketServerLocations(bucketServerLocations, out);
  }
  
  public static HashSet/*<ServerLocation>*/ readServerLocationSet(DataInput in) throws IOException, ClassNotFoundException {
    int size = in.readInt();
    if(size < 0) {
      return null;
    }
    HashSet serverLocations  = new HashSet(size);
    for(int i = 0; i < size; i++) {
      ServerLocation next = new ServerLocation();
      InternalDataSerializer.invokeFromData(next, in);
      serverLocations.add(next);
    }
    return serverLocations;
  }
  
  public static HashSet<BucketServerLocation66> readBucketServerLocationSet(DataInput in) throws IOException, ClassNotFoundException {
    int size = in.readInt();
    if(size < 0) {
      return null;
    }
    HashSet bucketServerLocations  = new HashSet(size);
    for(int i = 0; i < size; i++) {
      BucketServerLocation66 next = new BucketServerLocation66();
      InternalDataSerializer.invokeFromData(next, in);
      bucketServerLocations.add(next);
    }
    return bucketServerLocations;
  }
  

  private SerializationHelper() {
  }

}
