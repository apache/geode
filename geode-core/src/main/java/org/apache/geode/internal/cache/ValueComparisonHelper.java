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
package org.apache.geode.internal.cache;

import java.io.IOException;
import java.util.Arrays;

import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.offheap.StoredObject;
import org.apache.geode.internal.offheap.annotations.Unretained;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxSerializationException;
import org.apache.geode.pdx.PdxSerializer;

public class ValueComparisonHelper {

  public static boolean checkEquals(@Unretained Object v1, @Unretained Object v2,
      boolean isCompressedOffHeap, InternalCache cache) {
    // need to give PdxInstance#equals priority
    if (v1 instanceof PdxInstance) {
      return checkPdxEquals((PdxInstance) v1, v2, cache);
    } else if (v2 instanceof PdxInstance) {
      return checkPdxEquals((PdxInstance) v2, v1, cache);
    } else if (v1 instanceof StoredObject) {
      return checkOffHeapEquals((StoredObject) v1, v2, cache);
    } else if (v2 instanceof StoredObject) {
      return checkOffHeapEquals((StoredObject) v2, v1, cache);
    } else if (v1 instanceof CachedDeserializable) {
      return checkCDEquals((CachedDeserializable) v1, v2, isCompressedOffHeap, cache);
    } else if (v2 instanceof CachedDeserializable) {
      return checkCDEquals((CachedDeserializable) v2, v1, isCompressedOffHeap, cache);
    } else {
      return basicEquals(v1, v2);
    }
  }

  static boolean basicEquals(Object v1, Object v2) {
    if (v2 != null) {
      if (v2.getClass().isArray()) {
        // fix for 52093
        if (v2 instanceof byte[]) {
          if (v1 instanceof byte[]) {
            return Arrays.equals((byte[]) v2, (byte[]) v1);
          } else {
            return false;
          }
        } else if (v2 instanceof Object[]) {
          if (v1 instanceof Object[]) {
            return Arrays.deepEquals((Object[]) v2, (Object[]) v1);
          } else {
            return false;
          }
        } else if (v2 instanceof int[]) {
          if (v1 instanceof int[]) {
            return Arrays.equals((int[]) v2, (int[]) v1);
          } else {
            return false;
          }
        } else if (v2 instanceof long[]) {
          if (v1 instanceof long[]) {
            return Arrays.equals((long[]) v2, (long[]) v1);
          } else {
            return false;
          }
        } else if (v2 instanceof boolean[]) {
          if (v1 instanceof boolean[]) {
            return Arrays.equals((boolean[]) v2, (boolean[]) v1);
          } else {
            return false;
          }
        } else if (v2 instanceof short[]) {
          if (v1 instanceof short[]) {
            return Arrays.equals((short[]) v2, (short[]) v1);
          } else {
            return false;
          }
        } else if (v2 instanceof char[]) {
          if (v1 instanceof char[]) {
            return Arrays.equals((char[]) v2, (char[]) v1);
          } else {
            return false;
          }
        } else if (v2 instanceof float[]) {
          if (v1 instanceof float[]) {
            return Arrays.equals((float[]) v2, (float[]) v1);
          } else {
            return false;
          }
        } else if (v2 instanceof double[]) {
          if (v1 instanceof double[]) {
            return Arrays.equals((double[]) v2, (double[]) v1);
          } else {
            return false;
          }
        }
        // fall through and call equals method
      }
      return v2.equals(v1);
    } else {
      return v1 == null;
    }
  }

  /**
   * This method fixes bug 43643
   */
  private static boolean checkPdxEquals(PdxInstance pdx, Object obj, InternalCache cache) {
    if (!(obj instanceof PdxInstance)) {
      // obj may be a CachedDeserializable in which case we want to convert it to a PdxInstance even
      // if we are not readSerialized.
      if (obj instanceof CachedDeserializable) {
        CachedDeserializable cdObj = (CachedDeserializable) obj;
        if (!cdObj.isSerialized()) {
          // obj is actually a byte[] which will never be equal to a PdxInstance
          return false;
        }
        Object cdVal = cdObj.getValue();
        if (cdVal instanceof byte[]) {
          byte[] cdValBytes = (byte[]) cdVal;
          PdxInstance pi = InternalDataSerializer.readPdxInstance(cdValBytes, cache);
          if (pi != null) {
            return pi.equals(pdx);
          } else {
            // since obj is serialized as something other than pdx it must not equal our pdx
            return false;
          }
        } else {
          // remove the cd wrapper so that obj is the actual value we want to compare.
          obj = cdVal;
        }
      }
      if (obj != null && obj.getClass().getName().equals(pdx.getClassName())) {
        PdxSerializer pdxSerializer;
        if (obj instanceof PdxSerializable) {
          pdxSerializer = null;
        } else {
          pdxSerializer = cache.getPdxSerializer();
        }
        if (pdxSerializer != null || obj instanceof PdxSerializable) {
          // try to convert obj to a PdxInstance
          try (HeapDataOutputStream hdos = new HeapDataOutputStream(KnownVersion.CURRENT)) {
            if (InternalDataSerializer.autoSerialized(obj, hdos)
                || InternalDataSerializer.writePdx(hdos, cache, obj, pdxSerializer)) {
              PdxInstance pi = InternalDataSerializer.readPdxInstance(hdos.toByteArray(), cache);
              if (pi != null) {
                obj = pi;
              }
            }
          } catch (IOException | PdxSerializationException ignore) {
            // we are not able to convert it so just fall through
          }
        }
      }
    }
    return basicEquals(obj, pdx);
  }

  private static boolean checkOffHeapEquals(@Unretained StoredObject ohVal, @Unretained Object obj,
      InternalCache cache) {
    if (ohVal.isSerializedPdxInstance()) {
      PdxInstance pi = InternalDataSerializer.readPdxInstance(ohVal.getSerializedValue(), cache);
      return ValueComparisonHelper.checkPdxEquals(pi, obj, cache);
    }
    if (obj instanceof StoredObject) {
      return ohVal.checkDataEquals((StoredObject) obj);
    } else {
      byte[] serializedObj;
      if (obj instanceof CachedDeserializable) {
        CachedDeserializable cdObj = (CachedDeserializable) obj;
        if (!ohVal.isSerialized()) {
          assert cdObj.isSerialized();
          return false;
        }
        serializedObj = cdObj.getSerializedValue();
      } else if (obj instanceof byte[]) {
        if (ohVal.isSerialized()) {
          return false;
        }
        serializedObj = (byte[]) obj;
      } else {
        if (!ohVal.isSerialized()) {
          return false;
        }
        if (obj == null || obj == Token.NOT_AVAILABLE || Token.isInvalidOrRemoved(obj)) {
          return false;
        }
        serializedObj = EntryEventImpl.serialize(obj);
      }
      return ohVal.checkDataEquals(serializedObj);
    }
  }

  private static boolean checkCDEquals(CachedDeserializable cd, Object obj,
      boolean isCompressedOffHeap, InternalCache cache) {
    if (!cd.isSerialized()) {
      // cd is an actual byte[].
      byte[] ba2;
      if (obj instanceof CachedDeserializable) {
        CachedDeserializable cdObj = (CachedDeserializable) obj;
        if (cdObj.isSerialized()) {
          return false;
        }
        ba2 = (byte[]) cdObj.getDeserializedForReading();
      } else if (obj instanceof byte[]) {
        ba2 = (byte[]) obj;
      } else {
        return false;
      }
      byte[] ba1 = (byte[]) cd.getDeserializedForReading();
      return Arrays.equals(ba1, ba2);
    }
    Object cdVal = cd.getValue();
    if (cdVal instanceof byte[]) {
      if (obj == null || obj == Token.NOT_AVAILABLE || Token.isInvalidOrRemoved(obj)) {
        return false;
      }
      byte[] cdValBytes = (byte[]) cdVal;
      PdxInstance pi = InternalDataSerializer.readPdxInstance(cdValBytes, cache);
      if (pi != null) {
        return ValueComparisonHelper.checkPdxEquals(pi, obj, cache);
      }
      if (isCompressedOffHeap) {
        // fix for bug 52248
        byte[] serializedObj;
        if (obj instanceof CachedDeserializable) {
          serializedObj = ((CachedDeserializable) obj).getSerializedValue();
        } else {
          serializedObj = EntryEventImpl.serialize(obj);
        }
        return Arrays.equals(cdValBytes, serializedObj);
      } else {
        /*
         * To be more compatible with previous releases do not compare the serialized forms here.
         * Instead deserialize and call the equals method.
         */
        Object deserializedObj;
        if (obj instanceof CachedDeserializable) {
          deserializedObj = ((CachedDeserializable) obj).getDeserializedForReading();
        } else {
          // TODO OPTIMIZE: Before serializing all of obj we could get the top
          // level class name of cdVal and compare it to the top level class name of obj.
          deserializedObj = obj;
        }
        return basicEquals(deserializedObj, cd.getDeserializedForReading());
      }
    } else {
      // prefer object form
      if (obj instanceof CachedDeserializable) {
        // TODO OPTIMIZE: Before deserializing all of obj we could get the top
        // class name of cdVal and the top level class name of obj and compare.
        obj = ((CachedDeserializable) obj).getDeserializedForReading();
      }
      return basicEquals(cdVal, obj);
    }
  }
}
