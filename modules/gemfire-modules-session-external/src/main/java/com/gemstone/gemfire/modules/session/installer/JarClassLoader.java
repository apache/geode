/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.session.installer;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Classloader, grabbed from the web, which allows finding classes in jars
 * within jars. This is used to check whether a listener, as found in web.xml,
 * is a ServletContextListener
 */
public class JarClassLoader extends URLClassLoader {

  public JarClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, parent);

    try {
      for (URL url : urls) {
        if (isJar(url.getFile())) {
          addJarResource(new File(url.getPath()));
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void addJarResource(File file) throws IOException {
    JarFile jarFile = new JarFile(file);
    addURL(file.toURL());
    Enumeration<JarEntry> jarEntries = jarFile.entries();
    while (jarEntries.hasMoreElements()) {
      JarEntry jarEntry = jarEntries.nextElement();
      if (!jarEntry.isDirectory() && isJar(jarEntry.getName())) {
        addJarResource(jarEntryAsFile(jarFile, jarEntry));
      }
    }
  }

  @Override
  protected synchronized Class<?> loadClass(String name, boolean resolve)
      throws ClassNotFoundException {
    try {
      Class<?> clazz = findLoadedClass(name);
      if (clazz == null) {
        clazz = findClass(name);
        if (resolve) {
          resolveClass(clazz);
        }
      }
      return clazz;
    } catch (ClassNotFoundException e) {
      return super.loadClass(name, resolve);
    }
  }

  private static void close(Closeable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private static boolean isJar(String fileName) {
    return fileName != null && (fileName.toLowerCase().endsWith(".jar") ||
        fileName.toLowerCase().endsWith(".war") ||
        fileName.toLowerCase().endsWith(".ear"));
  }

  private static File jarEntryAsFile(JarFile jarFile,
      JarEntry jarEntry) throws IOException {
    InputStream input = null;
    OutputStream output = null;
    try {
      String name = jarEntry.getName().replace('/', '_');
      int i = name.lastIndexOf(".");
      String extension = i > -1 ? name.substring(i) : "";
      File file = File.createTempFile(
          name.substring(0, name.length() - extension.length()) + ".",
          extension);
      file.deleteOnExit();
      input = jarFile.getInputStream(jarEntry);
      output = new FileOutputStream(file);
      int readCount;
      byte[] buffer = new byte[4096];
      while ((readCount = input.read(buffer)) != -1) {
        output.write(buffer, 0, readCount);
      }
      return file;
    } finally {
      close(input);
      close(output);
    }
  }

}
