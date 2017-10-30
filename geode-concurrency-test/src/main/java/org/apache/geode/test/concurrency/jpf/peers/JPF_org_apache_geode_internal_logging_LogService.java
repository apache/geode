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
package org.apache.geode.test.concurrency.jpf.peers;

import gov.nasa.jpf.annotation.MJI;
import gov.nasa.jpf.vm.MJIEnv;
import gov.nasa.jpf.vm.NativePeer;

import org.apache.geode.test.concurrency.jpf.logging.EmptyLogger;

public class JPF_org_apache_geode_internal_logging_LogService extends NativePeer {

  @MJI
  public void $clinit____V(MJIEnv env, int clsObjRef) {
    // Override LogService initialization, which tries to load some log4j classes
    // which in turn will try to read from zip files, etc...
  }

  @MJI
  public int getLogger__Ljava_lang_String_2__Lorg_apache_logging_log4j_Logger_2(MJIEnv env,
      int clsObjRef, int rString0) {
    return getLogger____Lorg_apache_logging_log4j_Logger_2(env, clsObjRef);
  }

  @MJI
  public int getLogger____Lorg_apache_logging_log4j_Logger_2(MJIEnv env, int clsObjRef) {
    int logger = env.newObject(EmptyLogger.class.getName());
    return logger;
  }
}
