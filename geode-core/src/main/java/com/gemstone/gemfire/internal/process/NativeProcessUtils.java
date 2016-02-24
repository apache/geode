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
package com.gemstone.gemfire.internal.process;

import com.gemstone.gemfire.internal.process.ProcessUtils.InternalProcessUtils;
import com.gemstone.gemfire.internal.shared.NativeCalls;

/**
 * Implementation of the {@link ProcessUtils} SPI that uses {@link NativeCalls}.
 * 
 * @since 8.0
 */
final class NativeProcessUtils implements InternalProcessUtils {
  
  private final static NativeCalls nativeCalls = NativeCalls.getInstance();
  
  NativeProcessUtils() {}
  
  @Override
  public boolean isProcessAlive(int pid) {
    return nativeCalls.isProcessActive(pid);
  }
  
  @Override
  public boolean killProcess(int pid) {
    return nativeCalls.killProcess(pid);
  }
  
  @Override
  public boolean isAvailable() {
    return true;
  }

  @Override
  public boolean isAttachApiAvailable() {
    return false;
  }
}
