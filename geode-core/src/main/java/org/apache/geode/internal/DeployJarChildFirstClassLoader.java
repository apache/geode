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
import java.util.stream.Collectors;

/**
 * This class loader loads from itself/child first. It also crawls the rest of the deployed jars
 * when attempting
 * to do a class look up (for inter jar dependencies).
 *
 * As new jars versions of jars are deployed, the jar deployer does a little book keeping in the
 * latestJarNamesToClassLoader map.
 * This map is then used to ignore any old versions of a jar. For example if we have deployed
 * foov2.jar we should no longer crawl
 * foov1.jar when doing class look ups (even in the inter jar dependency case)
 *
 */
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
    Class c;
    if (thisIsOld()) {
      c = searchParent(name);
    } else {
      c = super.loadClass(name, resolve);
    }
    if (c == null) {
      for (DeployJarChildFirstClassLoader sibling : latestJarNamesToClassLoader.values().stream()
          .filter(s -> s != null).collect(Collectors.toList())) {
        try {
          c = sibling.findClass(name);
          if (c != null) {
            break;
          }
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
        }
      }
    }
    return c;
  }

  @Override
  public Class findClass(String name) throws ClassNotFoundException {
    if (thisIsOld()) {
      return null;
    }
    return super.findClass(name);
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

  boolean thisIsOld() {
    return latestJarNamesToClassLoader.get(jarName) != this;
  }

  public String getJarName() {
    return jarName;
  }
}
