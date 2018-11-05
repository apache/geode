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
package org.apache.geode;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.UUID;

import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.WritablePdxInstance;
import org.apache.geode.pdx.internal.PdxUnreadData;

/**
 * A static helper for optimally creating copies. Creating copies of cache values provides improved
 * concurrency as well as isolation. For transactions, creating a copy is the guaranteed way to
 * enforce "Read Committed" isolation on changes to cache <code>Entries</code>.
 *
 * <p>
 * Here is a simple example of how to use <code>CopyHelper.copy</code>
 *
 * <pre>
 * Object o = r.get("stringBuf");
 * StringBuffer s = (StringBuffer) CopyHelper.copy(o);
 * s.append("... and they lived happily ever after.  The End.");
 * r.put("stringBuf", s);
 * </pre>
 *
 * @see Cloneable
 * @see Serializable
 * @see DataSerializer
 * @see org.apache.geode.cache.Cache#setCopyOnRead
 * @see org.apache.geode.cache.CacheTransactionManager
 *
 * @since GemFire 4.0
 */

public final class CopyHelper {

  // no instances allowed
  private CopyHelper() {}

  /**
   * Return true if the given object is an instance of a well known immutable class. The well known
   * classes are:
   * <ul>
   * <li>String
   * <li>Byte
   * <li>Character
   * <li>Short
   * <li>Integer
   * <li>Long
   * <li>Float
   * <li>Double
   * <li>BigInteger
   * <li>BigDecimal
   * <li>UUID
   * <li>PdxInstance but not WritablePdxInstance
   * </ul>
   *
   * @param o the object to check
   * @return true if o is an instance of a well known immutable class.
   * @since GemFire 6.6.2
   */
  public static boolean isWellKnownImmutableInstance(Object o) {
    if (o instanceof String) {
      return true;
    }
    if (o instanceof Number) {
      if (o instanceof Integer)
        return true;
      if (o instanceof Long)
        return true;
      if (o instanceof Byte)
        return true;
      if (o instanceof Short)
        return true;
      if (o instanceof Float)
        return true;
      if (o instanceof Double)
        return true;
      // subclasses of non-final classes may be mutable
      if (o.getClass().equals(BigInteger.class))
        return true;
      if (o.getClass().equals(BigDecimal.class))
        return true;
    }
    if (o instanceof PdxInstance && !(o instanceof WritablePdxInstance)) {
      // no need to copy since it is immutable
      return true;
    }
    if (o instanceof Character)
      return true;
    if (o instanceof UUID)
      return true;
    return false;
  }

  /**
   * <p>
   * Makes a copy of the specified object. The object returned is not guaranteed to be a deep copy
   * of the original object, as explained below.
   *
   * <p>
   * Copies can only be made if the original is a <tt>Cloneable</tt> or serializable by GemFire. If
   * o is a {@link #isWellKnownImmutableInstance(Object) well known immutable instance} then it will
   * be returned without copying it.
   *
   * <p>
   * If the argument o is an instance of {@link java.lang.Cloneable}, a copy is made by invoking
   * <tt>clone</tt> on it. Note that not all implementations of <tt>clone</tt> make deep copies
   * (e.g. {@link java.util.HashMap#clone HashMap.clone}). Otherwise, if the argument is not an
   * instance of <tt>Cloneable</tt>, a copy is made using serialization: if GemFire serialization is
   * implemented, it is used; otherwise, java serialization is used.
   *
   * <p>
   * The difference between this method and {@link #deepCopy(Object) deepCopy}, is that this method
   * uses <tt>clone</tt> if available, whereas <tt>deepCopy</tt> does not. As a result, for
   * <tt>Cloneable</tt> objects copied using this method, how deep a copy the returned object is
   * depends on its implementation of <tt>clone</tt>.
   *
   * @param o the original object that a copy is needed of
   * @return the new instance that is a copy of of the original
   * @throws CopyException if copying fails because a class could not be found or could not be
   *         serialized.
   * @see #deepCopy(Object)
   * @since GemFire 4.0
   */
  @SuppressWarnings("unchecked")
  public static <T> T copy(T o) {
    T copy = null;
    try {
      if (o == null) {
        return null;
      } else if (o instanceof Token) {
        return o;
      } else {
        if (isWellKnownImmutableInstance(o))
          return o;
        if (o instanceof Cloneable) {
          try {
            // Note that Object.clone is protected so we need to use reflection
            // to call clone even though this object implements Cloneable
            Class<?> c = o.getClass();
            // By convention, the user should make the clone method public.
            // But even if they don't, let's go ahead and use it.
            // The other problem is that if the class is private, we still
            // need to make the method accessible even if the method is public,
            // because Object.clone is protected.
            Method m = c.getDeclaredMethod("clone", new Class[0]);
            m.setAccessible(true);
            copy = (T) m.invoke(o, new Object[0]);
            return copy;
          } catch (NoSuchMethodException | IllegalAccessException | SecurityException ignore) {
            // try using Serialization
          } catch (InvocationTargetException ex) {
            Throwable cause = ex.getTargetException();
            if (cause instanceof CloneNotSupportedException) {
              // try using Serialization
            } else {
              throw new CopyException("Clone failed.",
                  cause != null ? cause : ex);
            }
          }
        } else if (o instanceof CachedDeserializable) {
          copy = (T) ((CachedDeserializable) o).getDeserializedWritableCopy(null, null);
          return copy;
        }
        // Copy using serialization
        copy = doDeepCopy(o);
        return copy;
      }
    } finally {
      if (copy != null) {
        PdxUnreadData.copy(o, copy);
      }
    }
  }

  /**
   * Makes a deep copy of the specified object o using serialization, so the object has to be
   * serializable by GemFire.
   *
   * <p>
   * If o is a {@link #isWellKnownImmutableInstance(Object) well known immutable instance} then it
   * will be returned without copying it.
   *
   * <p>
   * The passed in object is serialized in memory, and then deserialized into a new instance, which
   * is returned. If GemFire serialization is implemented for the object, it is used; otherwise,
   * java serialization is used.
   *
   * @param o the original object to be copied
   * @return the new instance that is a copy of the original
   * @throws CopyException if copying fails because a class could not be found or could not be
   *         serialized
   * @see #copy(Object)
   */
  public static <T> T deepCopy(T o) {
    T copy = null;
    try {
      if (o == null) {
        return null;
      } else if (o instanceof Token || isWellKnownImmutableInstance(o)) {
        return o;
      } else {
        copy = doDeepCopy(o);
        return copy;
      }
    } finally {
      if (copy != null) {
        PdxUnreadData.copy(o, copy);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T doDeepCopy(T o) {
    try {
      HeapDataOutputStream hdos = new HeapDataOutputStream(Version.CURRENT);
      DataSerializer.writeObject(o, hdos);
      return (T) DataSerializer.readObject(new DataInputStream(hdos.getInputStream()));
    } catch (ClassNotFoundException ex) {
      throw new CopyException(
          String.format("Copy failed on instance of %s", o.getClass()),
          ex);
    } catch (IOException ex) {
      throw new CopyException(
          String.format("Copy failed on instance of %s", o.getClass()),
          ex);
    }

  }
}
