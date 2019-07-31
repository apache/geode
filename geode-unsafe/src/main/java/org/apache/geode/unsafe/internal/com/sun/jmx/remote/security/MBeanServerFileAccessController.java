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

package org.apache.geode.unsafe.internal.com.sun.jmx.remote.security;

import java.io.IOException;
import java.util.Properties;

import javax.management.MBeanServer;

public class MBeanServerFileAccessController
    extends com.sun.jmx.remote.security.MBeanServerFileAccessController {
  public MBeanServerFileAccessController(String accessFileName) throws IOException {
    super(accessFileName);
  }

  public MBeanServerFileAccessController(String accessFileName, MBeanServer mbs)
      throws IOException {
    super(accessFileName, mbs);
  }

  public MBeanServerFileAccessController(Properties accessFileProps) throws IOException {
    super(accessFileProps);
  }

  public MBeanServerFileAccessController(Properties accessFileProps,
      MBeanServer mbs) throws IOException {
    super(accessFileProps, mbs);
  }
}
