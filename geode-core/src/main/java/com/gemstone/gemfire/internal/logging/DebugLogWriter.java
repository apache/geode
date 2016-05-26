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
package com.gemstone.gemfire.internal.logging;


import com.gemstone.gemfire.i18n.StringId;

/**
 * A log writer that logs all types of log messages as a warning.
 * Intended usage was for individual classes that had their
 * own logger reference and switched it for debugging purposes.
 * e.g.
 *  <pre>
 *  Foo() { // constructor for class Foo
 *    if (Boolean.getBoolean(getClass().getName() + "-logging")) {
 *      this.logger = new DebugLogWriter((LogWriterImpl) getCache().getLogger(), getClass());
 *    } else {
 *      this.logger = pr.getCache().getLogger();
 *    }
 *  }
 *  </pre>
 * 
 * @since GemFire 5.0
 */
final public class DebugLogWriter extends LogWriterImpl
{
  private final LogWriterImpl realLogWriter;
  private final String prefix;
  public DebugLogWriter(LogWriterImpl lw, Class c) {
    this.realLogWriter = lw;
    this.prefix = c.getName() + ":";
//    this.realLogWriter.config(LocalizedStrings.DebugLogWriter_STARTED_USING_CLASS_LOGGER_FOR__0, getClass().getName());
  }

  @Override
  public int getLogWriterLevel()
  {
    return ALL_LEVEL;
  }

  @Override
  public void setLogWriterLevel(int newLevel) {
    throw new UnsupportedOperationException("Cannot restrict log level");
  }
  
  @Override
  public void put(int level, String msg, Throwable exception) {
    this.realLogWriter.put(WARNING_LEVEL, new StringBuilder(this.prefix).append(" level ").append(levelToString(level)).append(" ").append(msg).toString(), exception);
  }

  /**
   * Handles internationalized log messages.
   * @param params each Object has toString() called and substituted into the msg
   * @see com.gemstone.gemfire.i18n.StringId
   * @since GemFire 6.0
   */
  @Override
  public void put(int msgLevel, StringId msgId, Object[] params, Throwable exception)
  {
    String msg = new StringBuilder(this.prefix).append(" level ").append(levelToString(msgLevel)).append(" ").append(msgId.toLocalizedString(params)).toString();
    this.realLogWriter.put(WARNING_LEVEL, msg, exception);
  }

  @Override
  public boolean configEnabled()
  {
    return true;
  }

  @Override
  public boolean fineEnabled()
  {
    return true;
  }

  @Override
  public boolean finerEnabled()
  {
    return true;
  }

  @Override
  public boolean finestEnabled()
  {
    return true;
  }

  @Override
  public boolean infoEnabled()
  {
    return true;
  }

  @Override
  public boolean severeEnabled()
  {
    return true;
  }

  @Override
  public boolean warningEnabled()
  {
    return true;
  }
  
  @Override
  public String getConnectionName() {
    return null;
  }
  
}
