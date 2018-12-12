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

package org.apache.geode.internal.cache.tier.sockets;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UTFDataFormatException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.util.BlobHelper;

/**
 * <code>CacheServerHelper</code> is a static class that provides helper methods for the CacheServer
 * classes.
 *
 * @since GemFire 3.5
 */
public class CacheServerHelper {

  public static void setIsDefaultServer(CacheServer server) {
    if (server instanceof CacheServerImpl) {
      ((CacheServerImpl) server).setIsDefaultServer();
    }
  }

  public static boolean isDefaultServer(CacheServer server) {
    if (!(server instanceof CacheServerImpl)) {
      return false;
    }
    return ((CacheServerImpl) server).isDefaultServer();
  }

  public static byte[] serialize(Object obj) throws IOException {
    return serialize(obj, false);
  }

  public static byte[] serialize(Object obj, boolean zipObject) throws IOException {
    return zipObject ? zip(obj) : BlobHelper.serializeToBlob(obj);
  }

  public static Object deserialize(byte[] blob) throws IOException, ClassNotFoundException {
    return deserialize(blob, false);
  }

  public static Object deserialize(byte[] blob, boolean unzipObject)
      throws IOException, ClassNotFoundException {
    return unzipObject ? unzip(blob) : BlobHelper.deserializeBlob(blob);
  }

  public static Object deserialize(byte[] blob, Version version, boolean unzipObject)
      throws IOException, ClassNotFoundException {
    return unzipObject ? unzip(blob) : BlobHelper.deserializeBlob(blob, version, null);
  }

  public static byte[] zip(Object obj) throws IOException {
    // logger.info("CacheServerHelper: Zipping object to blob: " + obj);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    GZIPOutputStream gz = new GZIPOutputStream(baos);
    ObjectOutputStream oos = new ObjectOutputStream(gz);
    oos.writeObject(obj);
    oos.flush();
    oos.close();
    byte[] blob = baos.toByteArray();
    // logger.info("CacheServerHelper: Zipped object to blob: " + blob);
    return blob;
  }

  public static Object unzip(byte[] blob) throws IOException, ClassNotFoundException {
    // logger.info("CacheServerHelper: Unzipping blob to object: " + blob);
    ByteArrayInputStream bais = new ByteArrayInputStream(blob);
    GZIPInputStream gs = new GZIPInputStream(bais);
    ObjectInputStream ois = new ObjectInputStream(gs);
    Object obj = ois.readObject();
    // logger.info("CacheServerHelper: Unzipped blob to object: " + obj);
    ois.close();
    bais.close();
    return obj;
  }


  /**
   * The logic used here is based on java's DataInputStream.writeUTF() from the version 1.6.0_10.
   *
   * @return byte[]
   */
  public static byte[] toUTF(String s) {
    HeapDataOutputStream hdos = new HeapDataOutputStream(s);
    return hdos.toByteArray();
  }

  /**
   * The logic used here is based on java's DataInputStream.readUTF() from the version 1.6.0_10.
   *
   */
  public static String fromUTF(byte[] bytearr) {
    int utflen = bytearr.length;
    int c, char2, char3;
    int count = 0;
    int chararr_count = 0;

    char[] chararr = new char[utflen];

    while (count < utflen) {
      c = (int) bytearr[count] & 0xff;
      if (c > 127)
        break;
      count++;
      chararr[chararr_count++] = (char) c;
    }

    while (count < utflen) {
      c = (int) bytearr[count] & 0xff;
      switch (c >> 4) {
        case 0:
        case 1:
        case 2:
        case 3:
        case 4:
        case 5:
        case 6:
        case 7:
          /* 0xxxxxxx */
          count++;
          chararr[chararr_count++] = (char) c;
          break;
        case 12:
        case 13:
          /* 110x xxxx 10xx xxxx */
          count += 2;
          if (count > utflen) {
            throw new RuntimeException(
                "UTF-8 Exception malformed input",
                new UTFDataFormatException("malformed input: partial character at end"));
          }
          char2 = (int) bytearr[count - 1];
          if ((char2 & 0xC0) != 0x80)
            throw new RuntimeException("malformed input around byte " + count);
          chararr[chararr_count++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
          break;
        case 14:
          /* 1110 xxxx 10xx xxxx 10xx xxxx */
          count += 3;
          if (count > utflen) {
            throw new RuntimeException(
                "UTF-8 Exception malformed input",
                new UTFDataFormatException("malformed input: partial character at end"));
          }
          char2 = (int) bytearr[count - 2];
          char3 = (int) bytearr[count - 1];
          if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80)) {
            throw new RuntimeException(
                "UTF-8 Exception malformed input",
                new UTFDataFormatException("malformed input around byte " + (count - 1)));
          }
          chararr[chararr_count++] =
              (char) (((c & 0x0F) << 12) | ((char2 & 0x3F) << 6) | ((char3 & 0x3F) << 0));
          break;
        default:
          /* 10xx xxxx, 1111 xxxx */
          throw new RuntimeException(
              "UTF-8 Exception malformed input",
              new UTFDataFormatException("malformed input around byte " + count));
      }
    }
    // The number of chars produced may be less than utflen
    return new String(chararr, 0, chararr_count);
  }

  private CacheServerHelper() {}
}
