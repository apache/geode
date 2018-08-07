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
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ScanResult;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionAdapter;

public class FunctionScanner {

  public Collection<String> findFunctionsInJar(File jarFile) throws IOException {
    ClassGraph fastClasspathScanner = new ClassGraph().disableDirScanning()
        .removeTemporaryFilesAfterScan().overrideClasspath(jarFile.getAbsolutePath());
    ScanResult scanResult = fastClasspathScanner.enableClassInfo().scan();

    Set<String> functionClasses = new HashSet<>();

    functionClasses.addAll(scanResult.getClassesImplementing(Function.class.getName()).getNames());
    functionClasses.addAll(scanResult.getSubclasses(FunctionAdapter.class.getName()).getNames());

    return functionClasses;
  }
}
