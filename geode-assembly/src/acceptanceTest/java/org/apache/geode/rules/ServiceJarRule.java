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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import org.apache.commons.io.IOUtils;
import org.junit.rules.ExternalResource;

import org.apache.geode.test.junit.rules.accessible.AccessibleTemporaryFolder;

public class ServiceJarRule extends ExternalResource {

  private final AccessibleTemporaryFolder temporaryFolder;

  public ServiceJarRule() {
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

  public <S> Path createJarFor(String jarName, Class<S> serviceClass,
      Class<? extends S> providerClass) {
    try {
      return newJar(jarName, serviceClass.getName(), providerClass).toAbsolutePath();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Path newJar(String jarName, String serviceName, Class providerClass) throws IOException {
    File jar = temporaryFolder.newFile(jarName);

    String className = providerClass.getName();
    String classAsPath = className.replace('.', '/') + ".class";
    InputStream stream = providerClass.getClassLoader().getResourceAsStream(classAsPath);
    byte[] bytes = IOUtils.toByteArray(stream);
    try (FileOutputStream out = new FileOutputStream(jar)) {
      JarOutputStream jarOutputStream = new JarOutputStream(out);

      // Add the class file to the JAR file
      JarEntry classEntry = new JarEntry(classAsPath);
      classEntry.setTime(System.currentTimeMillis());
      jarOutputStream.putNextEntry(classEntry);
      jarOutputStream.write(bytes);
      jarOutputStream.closeEntry();

      String metaInfPath = "META-INF/services/" + serviceName;

      JarEntry metaInfEntry = new JarEntry(metaInfPath);
      metaInfEntry.setTime(System.currentTimeMillis());
      jarOutputStream.putNextEntry(metaInfEntry);
      jarOutputStream.write(className.getBytes());
      jarOutputStream.closeEntry();

      jarOutputStream.close();
    }

    return jar.toPath();
  }
}
