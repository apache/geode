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
package org.apache.geode.internal.admin.remote;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.geode.annotations.internal.MakeNotStatic;

public class InspectionClasspathManager {
  @MakeNotStatic
  private static InspectionClasspathManager internalRef;
  private final Map pathsToLoaders = new HashMap();
  private final ThreadLocal oldClassLoader = new ThreadLocal();
  // private static final String DESER_JAR = "lib" + File.separator + "gemfire_j2ee.jar";
  // private static final String DEFAULT_LOADER = "";

  public static synchronized InspectionClasspathManager getInstance() {
    if (internalRef == null) {
      internalRef = new InspectionClasspathManager();
    }
    return internalRef;
  }

  public void jumpToModifiedClassLoader(String modifiedClasspath) {
    if (modifiedClasspath != null && modifiedClasspath.length() > 0) {
      // TODO Kirk and Darrel believe this is dead code that is never used
      ClassLoader current = Thread.currentThread().getContextClassLoader();
      oldClassLoader.set(current);
      synchronized (pathsToLoaders) {
        ClassLoader newClassLoader = (ClassLoader) pathsToLoaders.get(modifiedClasspath);
        if (newClassLoader == null) {
          URL[] urls = convertToURLs(modifiedClasspath);
          URLClassLoader userClassLoader = new URLClassLoader(urls, current);
          pathsToLoaders.put(modifiedClasspath, userClassLoader);
          newClassLoader = userClassLoader;
        }
        Thread.currentThread().setContextClassLoader(newClassLoader);
      }
    }
  }

  public void revertToOldClassLoader() {
    ClassLoader loader = (ClassLoader) oldClassLoader.get();
    if (loader != null) {
      Thread.currentThread().setContextClassLoader(loader);
      oldClassLoader.set(null);
    }
  }

  private URL[] convertToURLs(String classpath) {
    List urls = new ArrayList();
    // must accept both separators, not just the current system's separator
    StringTokenizer tokenizer = new StringTokenizer(classpath, ":;");
    while (tokenizer.hasMoreTokens()) {
      java.io.File f = new java.io.File(tokenizer.nextToken());
      try {
        f = f.getCanonicalFile();
      } catch (IOException ex) {
        continue; // ignore?
      }
      try {
        urls.add(f.toURL());
      } catch (MalformedURLException mue) {
        continue; // ignore?
      }
    }
    URL[] array = new URL[urls.size()];
    return (URL[]) urls.toArray(array);
  }

}
