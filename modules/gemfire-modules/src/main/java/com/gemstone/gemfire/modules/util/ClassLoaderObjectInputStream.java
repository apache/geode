/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

/**
 * This class is used when session attributes need to be reconstructed with a
 * new classloader.
 */
public class ClassLoaderObjectInputStream extends ObjectInputStream {

  private final ClassLoader loader;

  public ClassLoaderObjectInputStream(InputStream in,
      ClassLoader loader) throws IOException {
    super(in);
    this.loader = loader;
  }

  @Override
  public Class<?> resolveClass(
      ObjectStreamClass desc) throws ClassNotFoundException {
    return Class.forName(desc.getName(), false, loader);
  }
}
