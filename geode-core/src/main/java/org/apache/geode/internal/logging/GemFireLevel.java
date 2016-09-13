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
package org.apache.geode.internal.logging;

import java.util.logging.Level;

import org.apache.geode.i18n.LogWriterI18n;
import org.apache.geode.internal.i18n.LocalizedStrings;

/**
 * Extension that adds ERROR to the standard JDK logging level class.
 */
public class GemFireLevel extends Level {
  private static final long serialVersionUID = -8123818329485173242L;
  /**
   * ERROR is a message level indicating a problem.
   * Unlike WARNING its not a potential problem.
   * Unlike SEVERE its not terminal.
   * 
   * In general ERROR messages should describe events that are of
   * considerable importance but will not prevent program
   * execution. They should be reasonably intelligible to end users
   * and to system administrators. This level is initialized to 950.
   */
  public static final Level ERROR = new GemFireLevel("error", InternalLogWriter.ERROR_LEVEL);
  
  public static Level create(int code) {
    switch (code) {
    case InternalLogWriter.ALL_LEVEL: return ALL;
    case InternalLogWriter.FINEST_LEVEL: return FINEST;
    case InternalLogWriter.FINER_LEVEL: return FINER;
    case InternalLogWriter.FINE_LEVEL: return FINE;
    case InternalLogWriter.CONFIG_LEVEL: return CONFIG;
    case InternalLogWriter.INFO_LEVEL: return INFO;
    case InternalLogWriter.WARNING_LEVEL: return WARNING;
    case InternalLogWriter.ERROR_LEVEL: return ERROR;
    case InternalLogWriter.SEVERE_LEVEL: return SEVERE;
    case InternalLogWriter.NONE_LEVEL: return OFF;
    default:
      throw new IllegalArgumentException(LocalizedStrings.GemFireLevel_UNEXPECTED_LEVEL_CODE_0.toLocalizedString(Integer.valueOf(code)));
    }
  }

  public static Level create(LogWriterI18n log) {
    if (log.finestEnabled()) return FINEST;
    if (log.finerEnabled()) return FINER;
    if (log.fineEnabled()) return FINE;
    if (log.configEnabled()) return CONFIG;
    if (log.infoEnabled()) return INFO;
    if (log.warningEnabled()) return WARNING;
    if (log.errorEnabled()) return ERROR;
    if (log.severeEnabled()) return SEVERE;

    return OFF;
  }
  
  private GemFireLevel(String name, int code) {
    super(name, code);
  }
  
  protected Object readResolve() {
    return create(this.intValue());
  }
}
