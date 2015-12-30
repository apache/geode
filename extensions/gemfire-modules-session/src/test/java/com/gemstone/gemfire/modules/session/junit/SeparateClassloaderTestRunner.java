/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.gemstone.gemfire.modules.session.junit;

import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

import java.net.URLClassLoader;

/**
 * @author StackOverflow
 */
public class SeparateClassloaderTestRunner extends BlockJUnit4ClassRunner {

  public SeparateClassloaderTestRunner(Class<?> clazz) throws InitializationError {
    super(getFromTestClassloader(clazz));
  }

  private static Class<?> getFromTestClassloader(Class<?> clazz) throws InitializationError {
    try {
      ClassLoader testClassLoader = new TestClassLoader();
      return Class.forName(clazz.getName(), true, testClassLoader);
    } catch (ClassNotFoundException e) {
      throw new InitializationError(e);
    }
  }

  public static class TestClassLoader extends URLClassLoader {
    public TestClassLoader() {
      super(((URLClassLoader)getSystemClassLoader()).getURLs());
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
      if (name.startsWith("com.gemstone.gemfire.modules.session.")) {
        return super.findClass(name);
      }
      return super.loadClass(name);
    }
  }
}
