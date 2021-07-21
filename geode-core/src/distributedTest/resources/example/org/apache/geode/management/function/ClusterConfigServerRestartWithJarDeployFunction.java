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
package example.org.apache.geode.management.function;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.pdx.PdxInstance;

public class ClusterConfigServerRestartWithJarDeployFunction implements Function {

  private static final Logger logger = LogService.getLogger();

  public static class Student {
    public Student() {}
  }

  @Override
  public void execute(FunctionContext context) {
    Object o = context.getArguments();
    Object studentObject = ((PdxInstance) o).getObject();

    logger.info("--->>> StudentObject = {}", studentObject);
    logger.info("--->>> Student        Loaded from {} using ClassLoader {}",
        jarForClass(studentObject.getClass()),
        studentObject.getClass().getClassLoader());
    logger.info("--->>> Class<Student> Loaded from {} using ClassLoader {}",
        jarForClass(Student.class),
        Student.class.getClassLoader());

    // Previously this would cause a ClassCastException if this was called after a reconnect
    Student student = (Student) ((PdxInstance) o).getObject();

    context.getResultSender().lastResult(true);
  }

  private String jarForClass(Class<?> clazz) {
    ClassLoader loader = clazz.getClassLoader();
    String className = clazz.getName();
    String from = loader.getResource(className.replace('.', '/') +
        ".class").toString();

    return from;
  }

  @Override
  public String getId() {
    return "student-function";
  }
}
