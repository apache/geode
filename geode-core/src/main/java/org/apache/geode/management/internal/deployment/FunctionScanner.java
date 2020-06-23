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
package org.apache.geode.management.internal.deployment;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.stream.Collectors;

import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;

import org.apache.geode.cache.execute.Function;

public class FunctionScanner {

  public Collection<String> findFunctionsInJar(File jarFile) throws IOException {
    URL jarFileUrl = jarFile.toURI().toURL();
    ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();

    ClassLoader jarClassLoader = new URLClassLoader(new URL[] {jarFileUrl});
    configurationBuilder.setUrls(jarFileUrl);
    configurationBuilder.setClassLoaders(new ClassLoader[] {jarClassLoader});
    Reflections reflections = new Reflections(configurationBuilder);

    return reflections.getSubTypesOf(Function.class)
        .stream()
        .map(Class::getCanonicalName)
        .collect(Collectors.toSet());
  }
}
