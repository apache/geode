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
 *
 */

package org.apache.geode.test.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import org.apache.commons.io.IOUtils;

public class JarUtils {
  public static Path createJarWithClasses(Path jarPath, Class<?>... classes) throws IOException {
    Path jarFile = Files.createFile(jarPath);
    try (OutputStream outputStream = Files.newOutputStream(jarFile);
        JarOutputStream jarOutputStream = new JarOutputStream(outputStream)) {
      Arrays.stream(classes)
          .forEach(clazz -> writeClassTo(clazz, jarOutputStream));
    }
    return jarFile;
  }

  private static void writeClassTo(Class<?> clazz, JarOutputStream jarOutputStream) {
    String className = clazz.getName();
    String classAsPath = className.replace('.', '/') + ".class";
    InputStream classInputStream = clazz.getClassLoader().getResourceAsStream(classAsPath);
    if (classInputStream == null) {
      throw new RuntimeException("No such class: " + clazz);
    }
    JarEntry classEntry = new JarEntry(classAsPath);
    classEntry.setTime(System.currentTimeMillis());
    try {
      byte[] classBytes = IOUtils.toByteArray(classInputStream);
      jarOutputStream.putNextEntry(classEntry);
      jarOutputStream.write(classBytes);
      jarOutputStream.closeEntry();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
