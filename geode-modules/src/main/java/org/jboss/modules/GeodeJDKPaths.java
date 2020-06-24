/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2014 Red Hat, Inc., and individual contributors
 * as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jboss.modules;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * A utility class which maintains the set of JDK paths. Makes certain assumptions about the
 * disposition of the
 * class loader used to load JBoss Modules; thus this class should only be used when booted up via
 * the "-jar" or "-cp"
 * switches.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 * @author <a href="mailto:ropalka@redhat.com">Richard Opalka</a>
 */
public final class GeodeJDKPaths {
  static final Set<String> JDK;

  static {
    Set<String> result = new TreeSet<>();
    processClassPathItem(System.getProperty("java.class.path"), result);
    if (result.size() == 0)
      throw new IllegalStateException("Something went wrong with system paths set up");
    JDK = Collections.unmodifiableSet(result);
  }

  private GeodeJDKPaths() {}

  static void processJar(final Set<String> pathSet, final File file) throws IOException {
    try (final ZipFile zipFile = new ZipFile(file)) {
      final Enumeration<? extends ZipEntry> entries = zipFile.entries();
      while (entries.hasMoreElements()) {
        final ZipEntry entry = entries.nextElement();
        final String name = entry.getName();
        final int lastSlash = name.lastIndexOf('/');
        if (lastSlash != -1) {
          pathSet.add(name.substring(0, lastSlash));
        }
      }
    }
  }

  public static void processClassPathItem(final String classPath, final Set<String> pathSet) {
    if (classPath == null)
      return;
    int start = 0, end;
    do {
      end = classPath.indexOf(File.pathSeparatorChar, start);
      String item = end == -1 ? classPath.substring(start) : classPath.substring(start, end);
      // if (! jarSet.contains(item)) {
      final File file = new File(item);
      if (file.isDirectory()) {
        processDirectory(pathSet, file);
      } else {
        try {
          processJar(pathSet, file);
        } catch (IOException ex) {
          // ignore
        }
      }
      // }
      start = end + 1;
    } while (end != -1);
  }

  static void processDirectory(final Set<String> pathSet, final File file) {
    for (File entry : file.listFiles()) {
      if (entry.isDirectory()) {
        processDirectory1(pathSet, entry, file.getPath());
      } else {
        final String parent = entry.getParent();
        if (parent != null)
          pathSet.add(parent);
      }
    }
  }

  private static void processDirectory1(final Set<String> pathSet, final File file,
      final String pathBase) {
    for (File entry : file.listFiles()) {
      if (entry.isDirectory()) {
        processDirectory1(pathSet, entry, pathBase);
      } else {
        String packagePath = entry.getParent();
        if (packagePath != null) {
          packagePath = packagePath.substring(pathBase.length()).replace('\\', '/');
          if (packagePath.startsWith("/")) {
            packagePath = packagePath.substring(1);
          }
          pathSet.add(packagePath);
        }
      }
    }
  }

  public static Set<String> getListPackagesFromJars(String classPathString) {
    Set<String> packageNames = new TreeSet<>();
    if (classPathString == null) {
      return packageNames;
    }
    String[] classPaths = classPathString.split(File.pathSeparator);
    Arrays.stream(classPaths).forEach(classPath -> {
      final File file = new File(classPath);
      if (!file.isDirectory()) {
        try {
          processJarForPackageNames(packageNames, file);
        } catch (IOException ex) {
          // ignore
        }
      }
    });
    return packageNames;
  }

  private static void processJarForPackageNames(Set<String> packageNames, File file)
      throws IOException {
    try (JarFile jarFile = new JarFile(file)) {
      try {
        Enumeration<JarEntry> entries = jarFile.entries();
        while (entries.hasMoreElements()) {
          JarEntry jarEntry = entries.nextElement();
          if (jarEntry == null) {
            break;
          }
          if ((jarEntry.getName().endsWith(".class"))) {
            String className = jarEntry.getName().replaceAll("/", "\\.");
            String myClass = className.substring(0, className.lastIndexOf('.'));
            packageNames.add(myClass.substring(0, myClass.lastIndexOf(".")));
          }
        }
      } catch (Exception e) {
        System.out.println("Oops.. Encounter an issue while parsing jar" + e.toString());
      }
    }
  }
}
