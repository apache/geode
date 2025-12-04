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
package org.apache.geode.management.internal.cli.shell.jline;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;

import org.jline.terminal.impl.DumbTerminal;

/**
 * Used when gfsh is run in Head Less mode. Doesn't support ANSI.
 * Updated for JLine 3.x: extends DumbTerminal for basic functionality
 *
 * @since GemFire 7.0
 */
public class GfshUnsupportedTerminal extends DumbTerminal {

  public GfshUnsupportedTerminal() throws IOException {
    this(System.in, System.out);
  }

  public GfshUnsupportedTerminal(InputStream input, OutputStream output) throws IOException {
    super("unsupported", "dumb", input, output, Charset.defaultCharset());
  }
}
