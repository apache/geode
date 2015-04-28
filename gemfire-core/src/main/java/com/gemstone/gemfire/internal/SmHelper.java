/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.internal;

import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.util.*;
import java.lang.reflect.*;
import java.io.InvalidClassException;

/**
 * This class defines general native utils.
 *
 * @author Darrel Schneider
 *
 */
public class SmHelper {

  static final boolean pureMode = PureJavaMode.isPure();

  /* ================== Native Methods =========================== */

  /**
   * Returns the size in bytes of a C pointer in this
   * shared library,  returns 4 for a 32 bit shared library,
   * and 8 for a 64 bit shared library
   */
  public static int pointerSizeBytes() {
    if (pureMode) {
      throw new IllegalStateException(LocalizedStrings.SmHelper_POINTERSIZEBYTES_UNAVAILABLE_IN_PURE_MODE.toLocalizedString());
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
    if (Thread.interrupted()) throw new InterruptedException();
    if (nanos <= 0) {
      return;
    }
    if (nanos >= 1000000) {
      Thread.sleep(nanos/1000000, (int)(nanos%1000000));
    } else {
      if (pureMode) {
        // fix for bug 35150
        Thread.yield();
      } else {
        _nanosleep((int)nanos);
      }
    }
  }

  /**
   * Returns the GemFire native code library's version string.
   */
  public static String getNativeVersion() {
    if (pureMode) {
      return LocalizedStrings.SmHelper_NATIVE_CODE_UNAVAILABLE.toLocalizedString();
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
   * Allocates a new JOM instance of <code>c</code> and
   * invokes the zero-argument constructor of <code>initClass</code>
   * on the new object to initialize it.  This takes the place of the
   * late, lamented <code>allocateNewObject</code> method of
   * <code>java.io.ObjectInputStream</code> in JDK 1.3.
   *
   * @throws InvalidClassException
   *         If <code>c</code> or <code>initClass</code> represents a
   *         primitive class.
   * @throws NoSuchMethodError
   *         If <code>initClass</code> doesn't have a zero-argument
   *         constructor 
   * @throws IllegalAccessException
   *         If the zero-argument constructor is not public (only for
   *         <code>Externalizable</code> classes in which
   *         <code>c.equals(initClass)</code>
   */
  public static Object allocateJOMObject(Class c, Class initClass) 
    throws IllegalAccessException, InvalidClassException {
    if (c.isPrimitive()) {
      throw new InvalidClassException(LocalizedStrings.SmHelper_IS_PRIMITIVE.toLocalizedString(), c.getName());

    } else if (initClass.isPrimitive()) {
      throw new InvalidClassException(LocalizedStrings.SmHelper_IS_PRIMITIVE.toLocalizedString(), initClass.getName());
    }

//    boolean hasZeroArgInit;
    if (!zeroArgConstructorCache.containsKey(initClass)) {
      try {
//        Constructor init = 
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

//     // Doesn't let us invoke the non-public constructors of inner
//     // classes 
//     boolean isPublic = 
//       ((Boolean) publicConstructorCache.get(initClass)).booleanValue();
//     if (c.equals(initClass) && !isPublic) {
//       throw new IllegalAccessException();
//     }

    return _allocateJOMObject(c, initClass);
  }

  /** 
   * Natively allocates a new JOM instance of <code>c</code> and
   * invokes the zero-argument constructor of <code>initClass</code>
   * on the new object to initialize it.  This takes the place of the
   * late, lamented <code>allocateNewObject</code> method of
   * <code>java.io.ObjectInputStream</code> in JDK 1.3.
   */
  private static native Object _allocateJOMObject(Class c, Class initClass);

  /**
   * Uses native code to set the value of an object JOM field
   */
  public static native void _setObjectField(Object o, Field field, Object newValue);
}
