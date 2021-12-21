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
package org.apache.geode.management.internal.unsafe;

import java.io.IOException;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.apache.geode.unsafe.internal.com.sun.jmx.remote.security.MBeanServerFileAccessController;

/**
 * This class extends existing properties file based accessController in order to implement read
 * only operations. In JMX all operations assume to be modifying the system hence requires
 * read-write access. But in GemFire some of the attributes are implemented as operations in order
 * to reduce federation cost due to large payloads of those attribute. These attributes are exposed
 * as operations but require readOnly access. This class filter those methods for read access.
 *
 */


public class ReadOpFileAccessController extends MBeanServerFileAccessController {

  public static final String readOnlyJMXOperations =
      "(^list.*|^fetch.*|^view.*|^show.*|^queryData.*)";

  public ReadOpFileAccessController(String accessFileName) throws IOException {
    super(accessFileName);
  }

  @Override
  public Object invoke(ObjectName name, String operationName, Object[] params, String[] signature)
      throws InstanceNotFoundException, MBeanException, ReflectionException {


    if (operationName.matches(readOnlyJMXOperations)) {
      checkRead();
      return getMBeanServer().invoke(name, operationName, params, signature);
    } else {
      return super.invoke(name, operationName, params, signature);
    }

  }

}
