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
package org.apache.geode.pdx;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.Version;

class NonDelegatingLoader extends ClassLoader {


  public NonDelegatingLoader(ClassLoader parent) {
    super(parent);
  }

  @Override
  public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    if (!name.contains("SeparateClassloaderPdx")) {
      return super.loadClass(name, resolve);
    }
    URL url = super.getResource(name.replace('.', '/') + ".class");
    if (url == null) {
      throw new ClassNotFoundException();
    }
    HeapDataOutputStream hoas = new HeapDataOutputStream(Version.CURRENT);
    InputStream classStream;
    try {
      classStream = url.openStream();
      while (true) {
        byte[] chunk = new byte[1024];
        int read = classStream.read(chunk);
        if (read < 0) {
          break;
        }
        hoas.write(chunk, 0, read);
      }
    } catch (IOException e) {
      throw new ClassNotFoundException("Error reading class", e);
    }

    Class clazz = defineClass(name, hoas.toByteBuffer(), null);
    if (resolve) {
      resolveClass(clazz);
    }
    return clazz;
  }

}
