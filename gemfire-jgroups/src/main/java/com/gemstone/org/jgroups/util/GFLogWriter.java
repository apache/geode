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
package com.gemstone.org.jgroups.util;

import java.io.StreamCorruptedException;

public interface GFLogWriter {

  boolean fineEnabled();

  boolean infoEnabled();

  boolean warningEnabled();
  
  boolean severeEnabled();

  
  void fine(String string);

  void fine(String string, Throwable e);


  void info(StringId str);
  
  void info(StringId str, Throwable e);

  void info(StringId str, Object arg);

  void info(StringId str, Object[] objects);

  void info(StringId str, Object arg, Throwable e);

  void warning(StringId str);

  void warning(StringId str, Object arg, Throwable thr);

  void warning(StringId str, Object[] args, Throwable thr);

  void warning(StringId str,
      Object arg);
  
  void warning(StringId str, Throwable e);

  void warning(
      StringId str,
      Object[] objects);

  void severe(StringId str);
  
  void severe(StringId str, Throwable e);

  void severe(
      StringId str,
      Object arg);

  void severe(StringId str, Object arg, Throwable exception);

  
}
