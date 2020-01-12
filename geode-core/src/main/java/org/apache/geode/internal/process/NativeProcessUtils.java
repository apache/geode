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
package org.apache.geode.internal.process;

import static org.apache.commons.lang3.Validate.isTrue;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.process.ProcessUtils.InternalProcessUtils;
import org.apache.geode.internal.shared.NativeCalls;

/**
 * Implementation of the {@link ProcessUtils} SPI that uses {@link NativeCalls}.
 *
 * @since GemFire 8.0
 */
class NativeProcessUtils implements InternalProcessUtils {

  @Immutable
  private static final NativeCalls nativeCalls = NativeCalls.getInstance();

  @Override
  public boolean isProcessAlive(final int pid) {
    isTrue(pid > 0, "Invalid pid '" + pid + "' specified");

    return nativeCalls.isProcessActive(pid);
  }

  @Override
  public boolean killProcess(final int pid) {
    isTrue(pid > 0, "Invalid pid '" + pid + "' specified");

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
