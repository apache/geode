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

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;

public class DeployJarChildFirstClassLoader extends ChildFirstClassLoader {

  private String jarName;
  public final HashMap<String, DeployJarChildFirstClassLoader> latestJarNamesToClassLoader;



  public DeployJarChildFirstClassLoader(
      HashMap<String, DeployJarChildFirstClassLoader> latestJarNamesToClassLoader, URL[] urls,
      String jarName, ClassLoader parent) {
    super(urls, parent);
    this.latestJarNamesToClassLoader = latestJarNamesToClassLoader;
    this.jarName = jarName;
    updateLatestJarNamesToClassLoaderMap();
  }

  private void updateLatestJarNamesToClassLoaderMap() {
    latestJarNamesToClassLoader.put(jarName, this);
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
    if (thisIsOld()) {
      return searchParent(name);
    } else {
      System.out.println("JASON loading class:" + name + " from " + jarName + ":" + this);
      return super.loadClass(name, resolve);
    }
  }

  @Override
  public URL findResource(String name) {
    if (thisIsOld()) {
      return null;
    }
    return super.findResource(name);
  }

  @Override
  public Enumeration<URL> findResources(final String name) throws IOException {
    if (thisIsOld()) {
      return Collections.enumeration(Collections.EMPTY_LIST);
    } else {
      return super.findResources(name);
    }
  }

  private boolean thisIsOld() {
    System.out.println("JASON" + jarName + ":" + latestJarNamesToClassLoader.get(jarName) + "::::"
        + this + " is this old?:" + (latestJarNamesToClassLoader.get(jarName) != this));
    return latestJarNamesToClassLoader.get(jarName) != this;
  }
}
