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

package org.apache.geode.javac;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;

public class SimpleFileManager extends ForwardingJavaFileManager<StandardJavaFileManager> {

  private final List<SimpleClassFile> compiled = new ArrayList<>();

  public SimpleFileManager(StandardJavaFileManager fileManager) {
    super(fileManager);
  }

  @Override
  public JavaFileObject getJavaFileForOutput(Location location, String className,
      JavaFileObject.Kind kind, FileObject sibling) {
    SimpleClassFile result = new SimpleClassFile(URI.create("string://" + className));
    compiled.add(result);
    return result;
  }

  public List<SimpleClassFile> getCompiled() {
    return compiled;
  }
}
