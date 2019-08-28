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
package org.apache.geode.test.compiler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ClassBuilderTest {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void writeJarFromClasses() throws IOException, ClassNotFoundException {
    File jar = tmpFolder.newFile("test.jar");
    URL[] url = new URL[] {jar.toURI().toURL()};
    ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();

    URLClassLoader classLoader = new URLClassLoader(url, systemClassLoader) {
      @Override
      public Class<?> loadClass(String name) throws ClassNotFoundException {
        try {
          return findClass(name);
        } catch (ClassNotFoundException e) {
          if (name.equals(Object.class.getName())) {
            return super.loadClass(name);
          }
        }
        return null;
      }
    };

    // write class to jar
    ClassBuilder.writeJarFromClasses(jar, TestObject.class, AnotherTestObject.class);

    // load class from the jar
    Class testObject = classLoader.loadClass(TestObject.class.getName());
    Class anotherTestObject = classLoader.loadClass(AnotherTestObject.class.getName());

    assertNotNull(testObject);
    assertNotNull(anotherTestObject);
    assertEquals(testObject.getClassLoader(), classLoader);
    assertEquals(anotherTestObject.getClassLoader(), classLoader);
  }

  public static class TestObject {
    public void forClassBuilderTest() {
      // this class is just for testing purposes
    }
  }

  public static class AnotherTestObject {
  }
}
