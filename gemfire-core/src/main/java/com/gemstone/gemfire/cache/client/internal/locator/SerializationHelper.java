/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
 * @author dsmith
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
