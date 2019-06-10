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
package org.apache.geode.internal.statistics;

import org.apache.geode.InternalGemFireException;

public final class OSVerifier {

  private final boolean isLinux;
  private final String currentOs;

  private OSVerifier() {
    currentOs = System.getProperty("os.name", "unknown");
    isLinux = (currentOs.startsWith("Linux") ? true : false);
    if (!isSupportedOs(currentOs)) {
      throw new InternalGemFireException(
          String.format("Unsupported OS %s. Only Linux(x86) OSs is supported.", currentOs));
    }
  }

  public static OSVerifier build() {
    return new OSVerifier();
  }

  private boolean isSupportedOs(String os) {
    boolean result = true;
    if (!os.startsWith("Linux") && !os.startsWith("Windows") && !os.equals("Mac OS X")
        && !os.equals("SunOS")) {
      result = false;
    }
    return result;
  }

  public boolean osIsLinux() {
    return this.isLinux;
  }

  public String getCurrentOS() {
    return this.currentOs;
  }
}
