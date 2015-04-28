/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.admin.remote;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

public class InspectionClasspathManager {
  private static InspectionClasspathManager internalRef;
  private Map pathsToLoaders = new HashMap();
  private ThreadLocal oldClassLoader = new ThreadLocal();
//  private static final String DESER_JAR = "lib" + File.separator + "gemfire_j2ee.jar";
//  private static final String DEFAULT_LOADER = "";

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
      synchronized(pathsToLoaders) {
        ClassLoader newClassLoader = (ClassLoader)pathsToLoaders.get(modifiedClasspath);
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
    ClassLoader loader = (ClassLoader)oldClassLoader.get();
    if (loader != null) {
      Thread.currentThread().setContextClassLoader(loader);
      oldClassLoader.set(null);
    }
  }

  private URL[] convertToURLs(String classpath) {
    List urls = new ArrayList();
    //must accept both separators, not just the current system's separator
    StringTokenizer tokenizer = new StringTokenizer(classpath, ":;");
    while(tokenizer.hasMoreTokens()) {      
      java.io.File f = new java.io.File(tokenizer.nextToken());
      try {
        f = f.getCanonicalFile();          
      } catch (IOException ex) {
        continue; //ignore?          
      }        
      try {
        urls.add(f.toURL());
      } catch (MalformedURLException mue){
        continue; //ignore?
      }
    }
    URL[] array = new URL[urls.size()];
    return (URL[])urls.toArray(array);
  }

}
