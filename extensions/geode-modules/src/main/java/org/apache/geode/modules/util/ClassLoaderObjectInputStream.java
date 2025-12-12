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
package org.apache.geode.modules.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

/**
 * This class is used when session attributes need to be reconstructed with a new classloader.
 * It now supports ObjectInputFilter for secure deserialization.
 */
public class ClassLoaderObjectInputStream extends ObjectInputStream {

  private final ClassLoader loader;

  /**
   * Constructs a ClassLoaderObjectInputStream with an ObjectInputFilter for secure deserialization.
   *
   * @param in the input stream to read from
   * @param loader the ClassLoader to use for class resolution
   * @param filter the ObjectInputFilter to validate deserialized classes (required for security)
   * @throws IOException if an I/O error occurs
   */
  public ClassLoaderObjectInputStream(InputStream in, ClassLoader loader, ObjectInputFilter filter)
      throws IOException {
    super(in);
    this.loader = loader;
    if (filter != null) {
      setObjectInputFilter(filter);
    }
  }

  /**
   * Legacy constructor for backward compatibility.
   *
   * @deprecated Use
   *             {@link #ClassLoaderObjectInputStream(InputStream, ClassLoader, ObjectInputFilter)}
   *             with a filter for secure deserialization
   */
  @Deprecated
  public ClassLoaderObjectInputStream(InputStream in, ClassLoader loader) throws IOException {
    super(in);
    this.loader = loader;
  }

  @Override
  public Class<?> resolveClass(ObjectStreamClass desc) throws ClassNotFoundException {
    Class<?> theClass;
    try {
      theClass = Class.forName(desc.getName(), false, loader);
    } catch (ClassNotFoundException cnfe) {
      theClass = Thread.currentThread().getContextClassLoader().loadClass(desc.getName());
    }
    return theClass;
  }
}
