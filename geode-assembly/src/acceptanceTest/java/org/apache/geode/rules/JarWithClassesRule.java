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

package org.apache.geode.rules;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Stream;

import org.apache.commons.io.IOUtils;
import org.junit.rules.ExternalResource;

import org.apache.geode.test.junit.rules.accessible.AccessibleTemporaryFolder;

public class JarWithClassesRule extends ExternalResource {

  private final AccessibleTemporaryFolder temporaryFolder;

  public JarWithClassesRule() {
    temporaryFolder = new AccessibleTemporaryFolder();
  }

  @Override
  protected void before() throws Throwable {
    temporaryFolder.before();
  }

  @Override
  protected void after() {
    temporaryFolder.after();
  }

  public Path createJarWithClasses(String jarName, Class<?>... classes) throws IOException {
    Path jarFile = temporaryFolder.newFile(jarName).toPath().toAbsolutePath().normalize();
    try (OutputStream outputStream = Files.newOutputStream(jarFile);
        JarOutputStream jarOutputStream = new JarOutputStream(outputStream)) {
      Stream.of(classes)
          .forEach(c -> writeClassTo(c, jarOutputStream));
    }
    return jarFile;
  }

  private void writeClassTo(Class<?> c, JarOutputStream jarOutputStream) {
    String className = c.getName();
    String classAsPath = className.replace('.', '/') + ".class";
    InputStream classInputStream = c.getClassLoader().getResourceAsStream(classAsPath);
    if (classInputStream == null) {
      throw new RuntimeException("No such class: " + c);
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
