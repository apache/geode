/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
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
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.pdx.internal.PdxInputStream;

/**
 * A "blob" is a serialized representation of an object into a byte[].
 * BlobHelper provides utility methods for
 * serializing and deserializing the object.
 * 
 * 
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
   * A blob is a serialized Object.  This method serializes the
   * object into a blob and returns the byte array that contains the blob.
   */
  public static byte[] serializeToBlob(Object obj, Version version)
  throws IOException
  {
    final long start = startSerialization();
    HeapDataOutputStream hdos = new HeapDataOutputStream(version);
    DataSerializer.writeObject(obj, hdos);
    byte[] result = hdos.toByteArray();
    endSerialization(start, result.length);
    return result;
  }

  /**
   * A blob is a serialized Object.  This method serializes the
   * object into the given HeapDataOutputStream.
   */
  public static void serializeTo(Object obj, HeapDataOutputStream hdos)
    throws IOException
  {
    final int startBytes = hdos.size();
    final long start = startSerialization();
    DataSerializer.writeObject(obj, hdos);
    endSerialization(start, hdos.size()-startBytes);
  }
                                                                        


  /**
   * A blob is a serialized Object.  This method 
   * returns the deserialized object.
   */
  public static Object deserializeBlob(byte[] blob) throws IOException,
      ClassNotFoundException {
    return deserializeBlob(blob, null, null);
  }

  /**
   * A blob is a serialized Object.  This method 
   * returns the deserialized object.
   */
  public static Object deserializeBlob(byte[] blob, Version version,
      ByteArrayDataInput in) throws IOException, ClassNotFoundException {
    Object result;
    final long start = startDeserialization();
    /*
    final StaticSystemCallbacks sysCb;
    if (version != null && (sysCb = GemFireCacheImpl.FactoryStatics
        .systemCallbacks) != null) {
      // may need to change serialized shape for SQLFire
      result = sysCb.fromVersion(blob, true, version, in);
    }
    else*/ if (blob.length > 0 && blob[0] == DSCODE.PDX) {
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
    // this causes a small performance drop in d-no-ack performance tests
//    if (dis.available() != 0) {
//      LogWriterI18n lw = InternalDistributedSystem.getLoggerI18n();
//      if (lw != null && lw.warningEnabled()) {
//        lw.warning(
//            LocalizedStrings.BlobHelper_DESERIALIZATION_OF_A_0_DID_NOT_READ_1_BYTES_THIS_INDICATES_A_LOGIC_ERROR_IN_THE_SERIALIZATION_CODE_FOR_THIS_CLASS,
//            new Object[] {((result!=null) ? result.getClass().getName() : "NULL"), Integer.valueOf(dis.available())});   
//            
//      }
//    }
    return result;
  }

  public static Object deserializeBuffer(ByteArrayDataInput in, int numBytes)
      throws IOException, ClassNotFoundException {
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
