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


import java.net.URL;
import java.net.URLClassLoader;

public class ChildFirstClassLoader extends URLClassLoader {

  public ChildFirstClassLoader() {
    super(new URL[] {});
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
   * We override the parent-first behavior established by java.lang.Classloader.
   */
  @Override
  protected Class loadClass(String name, boolean resolve) throws ClassNotFoundException {
    Class c = null;

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

    // if we could not find it, delegate to parent
    // Note that we don't attempt to catch any ClassNotFoundException
    if (c == null) {
      try {
        c = searchParent(name);
      } catch (ClassNotFoundException | NoClassDefFoundError cnfe) {
        // ignore
      }
    }

    if (resolve) {
      resolveClass(c);
    }

    return c;
  }

  @Override
  public URL getResource(String name) {
    URL url = null;
    if (url == null) {
      url = findResource(name);
    }
    if (url == null) {
      url = super.getResource(name);
    }
    return url;
  }

  protected Class searchParent(String name) throws ClassNotFoundException {
    Class c;
    if (getParent() != null) {
      c = getParent().loadClass(name);
    } else {
      c = getSystemClassLoader().loadClass(name);
    }
    return c;
  }
}
