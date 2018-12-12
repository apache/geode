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

package org.apache.geode.internal;

import java.io.InvalidClassException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;


/**
 * This class defines general native utils.
 *
 *
 */
public class SmHelper {

  static final boolean pureMode = PureJavaMode.isPure();

  /* ================== Native Methods =========================== */

  /**
   * Returns the size in bytes of a C pointer in this shared library, returns 4 for a 32 bit shared
   * library, and 8 for a 64 bit shared library
   */
  public static int pointerSizeBytes() {
    if (pureMode) {
      throw new IllegalStateException(
          "pointerSizeBytes unavailable in pure mode");
    } else {
      return _pointerSizeBytes();
    }
  }

  private static native int _pointerSizeBytes();


  /* ================== System Administration =========================== */

  /**
   * Sleeps for the specified number of nanoseconds.
   */
  private static native void _nanosleep(int nanos);

  /**
   * Sleeps for the specified number of nanoseconds.
   */
  public static void nanosleep(long nanos) throws InterruptedException {
    if (Thread.interrupted())
      throw new InterruptedException();
    if (nanos <= 0) {
      return;
    }
    if (nanos >= 1000000) {
      Thread.sleep(nanos / 1000000, (int) (nanos % 1000000));
    } else {
      if (pureMode) {
        // fix for bug 35150
        Thread.yield();
      } else {
        _nanosleep((int) nanos);
      }
    }
  }

  /**
   * Returns the GemFire native code library's version string.
   */
  public static String getNativeVersion() {
    if (pureMode) {
      return "native code unavailable";
    } else {
      return _getNativeVersion();
    }
  }

  private static native String _getNativeVersion();

  public static String getSystemId() {
    if (pureMode) {
      return "pureJavaMode";
    } else {
      return _getSystemId();
    }
  }

  private static native String _getSystemId();

  /** Cache of class -> has zero-arg constructor */
  private static Map zeroArgConstructorCache = new HashMap();

  /** Cache of class -> has public zero-arg constructor */
  private static Map publicConstructorCache = new HashMap();

  /**
   * Allocates a new JOM instance of <code>c</code> and invokes the zero-argument constructor of
   * <code>initClass</code> on the new object to initialize it. This takes the place of the late,
   * lamented <code>allocateNewObject</code> method of <code>java.io.ObjectInputStream</code> in JDK
   * 1.3.
   *
   * @throws InvalidClassException If <code>c</code> or <code>initClass</code> represents a
   *         primitive class.
   * @throws NoSuchMethodError If <code>initClass</code> doesn't have a zero-argument constructor
   * @throws IllegalAccessException If the zero-argument constructor is not public (only for
   *         <code>Externalizable</code> classes in which <code>c.equals(initClass)</code>
   */
  public static Object allocateJOMObject(Class c, Class initClass)
      throws IllegalAccessException, InvalidClassException {
    if (c.isPrimitive()) {
      throw new InvalidClassException("Is primitive",
          c.getName());

    } else if (initClass.isPrimitive()) {
      throw new InvalidClassException("Is primitive",
          initClass.getName());
    }

    // boolean hasZeroArgInit;
    if (!zeroArgConstructorCache.containsKey(initClass)) {
      try {
        // Constructor init =
        initClass.getDeclaredConstructor(new Class[0]);
        if (Modifier.isPublic(initClass.getModifiers())) {
          publicConstructorCache.put(initClass, Boolean.TRUE);

        } else {
          publicConstructorCache.put(initClass, Boolean.FALSE);
        }

      } catch (NoSuchMethodException ex) {
        zeroArgConstructorCache.put(initClass, Boolean.FALSE);
        throw new NoSuchMethodError();
      }

    } else {
      Boolean b = (Boolean) zeroArgConstructorCache.get(initClass);
      if (!b.booleanValue()) {
        throw new NoSuchMethodError();
      }
    }

    // // Doesn't let us invoke the non-public constructors of inner
    // // classes
    // boolean isPublic =
    // ((Boolean) publicConstructorCache.get(initClass)).booleanValue();
    // if (c.equals(initClass) && !isPublic) {
    // throw new IllegalAccessException();
    // }

    return _allocateJOMObject(c, initClass);
  }

  /**
   * Natively allocates a new JOM instance of <code>c</code> and invokes the zero-argument
   * constructor of <code>initClass</code> on the new object to initialize it. This takes the place
   * of the late, lamented <code>allocateNewObject</code> method of
   * <code>java.io.ObjectInputStream</code> in JDK 1.3.
   */
  private static native Object _allocateJOMObject(Class c, Class initClass);

  /**
   * Uses native code to set the value of an object JOM field
   */
  public static native void _setObjectField(Object o, Field field, Object newValue);
}
