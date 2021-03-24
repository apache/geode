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
package org.apache.geode.management.internal.cli.result;

import java.util.function.Supplier;

import org.apache.geode.management.internal.cli.shell.Gfsh;

class Screen {

  private static final String GFSH_TRIMSCRWIDTH_PROPERTY = "GFSH.TRIMSCRWIDTH";

  private final Supplier<Gfsh> gfshCurrentInstanceSupplier;

  Screen() {
    this(Gfsh::getCurrentInstance);
  }

  private Screen(Supplier<Gfsh> gfshCurrentInstanceSupplier) {
    this.gfshCurrentInstanceSupplier = gfshCurrentInstanceSupplier;
  }

  int trimWidthForScreen(int maxColLength) {
    if (shouldTrimColumns()) {
      int screenWidth = getScreenWidth();
      return Math.min(maxColLength, screenWidth);
    }
    return maxColLength;
  }

  int getScreenWidth() {
    Gfsh gfsh = gfshCurrentInstanceSupplier.get();
    if (gfsh == null) {
      return Gfsh.DEFAULT_WIDTH;
    }
    return gfsh.getTerminalWidth();
  }

  boolean shouldTrimColumns() {
    Gfsh gfsh = gfshCurrentInstanceSupplier.get();
    if (gfsh == null) {
      return Boolean.getBoolean(GFSH_TRIMSCRWIDTH_PROPERTY);
    }
    return Gfsh.DEFAULT_APP_RESULT_VIEWER.equals(gfsh.getEnvProperty(Gfsh.ENV_APP_RESULT_VIEWER))
        && !Gfsh.isInfoResult();
  }
}
