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
package com.gemstone.gemfire.modules.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

/**
 * This class is used when session attributes need to be reconstructed with a new classloader.
 */
public class ClassLoaderObjectInputStream extends ObjectInputStream {

  private final ClassLoader loader;

  public ClassLoaderObjectInputStream(InputStream in, ClassLoader loader) throws IOException {
    super(in);
    this.loader = loader;
  }

  @Override
  public Class<?> resolveClass(ObjectStreamClass desc) throws ClassNotFoundException {
    return Class.forName(desc.getName(), false, loader);
  }
}
