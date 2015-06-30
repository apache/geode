/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.session.filter;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

public class ChildFirstClassLoader extends URLClassLoader {

  public ChildFirstClassLoader() {
    super(new URL[]{});
  }

  public ChildFirstClassLoader(URL[] urls) {
    super(urls);
  }

  public ChildFirstClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, parent);
  }

  @Override
  public void addURL(URL url) {
    super.addURL(url);
  }

  @Override
  public Class loadClass(String name) throws ClassNotFoundException {
    return loadClass(name, false);
  }

  /**
   * We override the parent-first behavior established by
   * java.lang.Classloader.
   */
  @Override
  protected Class loadClass(String name, boolean resolve)
      throws ClassNotFoundException {
    Class c = null;

    if (name.startsWith("com.gemstone")) {
      // First, check if the class has already been loaded
      c = findLoadedClass(name);

      // if not loaded, search the local (child) resources
      if (c == null) {
        try {
          c = findClass(name);
        } catch (ClassNotFoundException cnfe) {
          // ignore
        }
      }
    }

    // if we could not find it, delegate to parent
    // Note that we don't attempt to catch any ClassNotFoundException
    if (c == null) {
      if (getParent() != null) {
        c = getParent().loadClass(name);
      } else {
        c = getSystemClassLoader().loadClass(name);
      }
    }

    if (resolve) {
      resolveClass(c);
    }

    return c;
  }
}
