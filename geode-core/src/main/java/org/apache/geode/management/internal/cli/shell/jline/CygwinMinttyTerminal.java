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
package org.apache.geode.management.internal.cli.shell.jline;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import jline.UnixTerminal;


/**
 * This is re-write of UnixTerminal with stty process spawn removed.
 * There is no process named stty in windows (non-cygwin process) so
 * that part is commented, also since erase is already applied within
 * gfsh script when running under cygwin backspaceDeleteSwitched is
 * hard-coded as true
 * 
 * To know exact changed please see UnixTerminal code.
 * 
 *
 */
public class CygwinMinttyTerminal extends UnixTerminal {
  
  
  String encoding = System.getProperty("input.encoding", "UTF-8");
  InputStreamReader replayReader;

  public CygwinMinttyTerminal() throws Exception{
  }

  @Override
  public void init() throws Exception{

  }

  @Override
  public void restore() throws Exception {
    reset();
  }
}
