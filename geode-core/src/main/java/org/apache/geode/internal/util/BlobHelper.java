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
package com.gemstone.gemfire.internal.util;

import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.ByteArrayDataInput;
import com.gemstone.gemfire.internal.DSCODE;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.offheap.StoredObject;
import com.gemstone.gemfire.internal.offheap.annotations.Unretained;
import com.gemstone.gemfire.pdx.internal.PdxInputStream;

/**
 * A "blob" is a serialized representation of an object into a byte[].
 * BlobHelper provides utility methods for serializing and deserializing the
 * object.
 *
 * TODO: compare performance with org.apache.commons.lang.SerializationUtils
 */
public class BlobHelper {

  /**
   * A blob is a serialized Object. This method serializes the object into a
   * blob and returns the byte array that contains the blob.
   */
  public static byte[] serializeToBlob(Object obj) throws IOException {
    return serializeToBlob(obj, null);
  }

  /**
   * A blob is a serialized Object.  This method serializes the object into a
   * blob and returns the byte array that contains the blob.
   */
  public static byte[] serializeToBlob(Object obj, Version version) throws IOException {
    final long start = startSerialization();
    HeapDataOutputStream hdos = new HeapDataOutputStream(version);
    DataSerializer.writeObject(obj, hdos);
    byte[] result = hdos.toByteArray();
    endSerialization(start, result.length);
    return result;
  }

  /**
   * A blob is a serialized Object.  This method serializes the object into
   * the given HeapDataOutputStream.
   */
  public static void serializeTo(Object obj, HeapDataOutputStream hdos) throws IOException {
    final int startBytes = hdos.size();
    final long start = startSerialization();
    DataSerializer.writeObject(obj, hdos);
    endSerialization(start, hdos.size()-startBytes);
  }

  /**
   * A blob is a serialized Object.  This method returns the deserialized
   * object.
   */
  public static Object deserializeBlob(byte[] blob) throws IOException, ClassNotFoundException {
    return deserializeBlob(blob, null, null);
  }

  /**
   * A blob is a serialized Object.  This method returns the deserialized
   * object.
   */
  public static Object deserializeBlob(byte[] blob, Version version, ByteArrayDataInput in) throws IOException, ClassNotFoundException {
    Object result;
    final long start = startDeserialization();
    if (blob.length > 0 && blob[0] == DSCODE.PDX) {
      // If the first byte of blob indicates a pdx then wrap
      // blob in a PdxInputStream instead.
      // This will prevent us from making a copy of the byte[]
      // every time we deserialize a PdxInstance.
      PdxInputStream is = new PdxInputStream(blob);
      result = DataSerializer.readObject(is);
    } else {
      // if we have a nested pdx then we want to make a copy
      // when a PdxInstance is created so that the byte[] will
      // just have the pdx bytes and not the outer objects bytes.
      if (in == null) {
        in = new ByteArrayDataInput();
      }
      in.initialize(blob, version);
      result = DataSerializer.readObject(in);
    }
    endDeserialization(start, blob.length);
    return result;
  }

  /**
   * A blob is a serialized Object.  This method returns the deserialized
   * object. If a PdxInstance is returned then it will refer to Chunk's
   * off-heap memory with an unretained reference.
   */
  public static @Unretained Object deserializeOffHeapBlob(StoredObject blob) throws IOException, ClassNotFoundException {
    Object result;
    final long start = startDeserialization();
    // For both top level and nested pdxs we just want a reference to this off-heap blob.
    // No copies.
    // For non-pdx we want a stream that will read directly from the chunk.
    PdxInputStream is = new PdxInputStream(blob);
    result = DataSerializer.readObject(is);
    endDeserialization(start, blob.getDataSize());
    return result;
  }

  /**
   * Unused
   */
  public static Object deserializeBuffer(ByteArrayDataInput in, int numBytes) throws IOException, ClassNotFoundException {
    final long start = startDeserialization();
    Object result = DataSerializer.readObject(in);
    endDeserialization(start, numBytes);
    return result;
  }

  private static long startSerialization() {
    long result = 0;
    DMStats stats = InternalDistributedSystem.getDMStats();
    if (stats != null) {
      result = stats.startSerialization();
    }
    return result;
  }
  
  private static void endSerialization(long start, int bytes) {
    DMStats stats = InternalDistributedSystem.getDMStats();
    if (stats != null) {
      stats.endSerialization(start, bytes);
    }
  }

  private static long startDeserialization() {
    long result = 0;
    DMStats stats = InternalDistributedSystem.getDMStats();
    if (stats != null) {
      result = stats.startDeserialization();
    }
    return result;
  }
  
  private static void endDeserialization(long start, int bytes) {
    DMStats stats = InternalDistributedSystem.getDMStats();
    if (stats != null) {
      stats.endDeserialization(start, bytes);
    }
  }
  
}
