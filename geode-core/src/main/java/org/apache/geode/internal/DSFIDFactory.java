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
package org.apache.geode.internal.serialization;

import java.io.DataInput;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.InternalDataSerializer;

/**
 * Factory for instances of DataSerializableFixedID instances.  Constructors must be registered
 * with this factory in order for it to deserialize anything.
 */
public class DSFIDFactory {

  @Immutable
  private final Constructor<?>[] dsfidMap = new Constructor<?>[256];

  @Immutable("This maybe should be wrapped in an unmodifiableMap?")
  private final Int2ObjectOpenHashMap dsfidMap2 = new Int2ObjectOpenHashMap(800);


  /** Register the constructor for a fixed ID class. */
  public void registerDSFID(int dsfid, Class dsfidClass) {
    try {
      Constructor<?> cons = dsfidClass.getConstructor((Class[]) null);
      cons.setAccessible(true);
      if (!cons.isAccessible()) {
        throw new InternalGemFireError(
            "default constructor not accessible " + "for DSFID=" + dsfid + ": " + dsfidClass);
      }
      if (dsfid >= Byte.MIN_VALUE && dsfid <= Byte.MAX_VALUE) {
        dsfidMap[dsfid + Byte.MAX_VALUE + 1] = cons;
      } else {
        dsfidMap2.put(dsfid, cons);
      }
    } catch (NoSuchMethodException nsme) {
      throw new InternalGemFireError(nsme);
    }
  }

  /**
   * Creates a DataSerializableFixedID or StreamableFixedID instance by deserializing it from the
   * data input.
   */
  public Object create(int dsfid, DataInput in) throws IOException, ClassNotFoundException {
    final Constructor<?> cons;
    if (dsfid >= Byte.MIN_VALUE && dsfid <= Byte.MAX_VALUE) {
      cons = dsfidMap[dsfid + Byte.MAX_VALUE + 1];
    } else {
      cons = (Constructor<?>) dsfidMap2.get(dsfid);
    }
    if (cons != null) {
      try {
        Object ds = cons.newInstance((Object[]) null);
        InternalDataSerializer.invokeFromData(ds, in);
        return ds;
      } catch (InstantiationException ie) {
        throw new IOException(ie.getMessage(), ie);
      } catch (IllegalAccessException iae) {
        throw new IOException(iae.getMessage(), iae);
      } catch (InvocationTargetException ite) {
        Throwable targetEx = ite.getTargetException();
        if (targetEx instanceof IOException) {
          throw (IOException) targetEx;
        } else if (targetEx instanceof ClassNotFoundException) {
          throw (ClassNotFoundException) targetEx;
        } else {
          throw new IOException(ite.getMessage(), targetEx);
        }
      }
    }
    throw new DSFIDNotFoundException("Unknown DataSerializableFixedID: " + dsfid, dsfid);
  }


  public Constructor<?>[] getDsfidmap() {
    return dsfidMap;
  }

  public Int2ObjectOpenHashMap getDsfidmap2() {
    return dsfidMap2;
  }

}
