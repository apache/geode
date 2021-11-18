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

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.net.URI;

import javax.tools.SimpleJavaFileObject;

public class InMemoryClassFile extends SimpleJavaFileObject {
  private final String name;
  private ByteArrayOutputStream out;

  public InMemoryClassFile(String name) {
    super(URI.create("string:///" + name.replace('.', '/') + Kind.CLASS.extension), Kind.CLASS);
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public OutputStream openOutputStream() {
    return out = new ByteArrayOutputStream();
  }

  public byte[] getByteContent() {
    return out.toByteArray();
  }
}
