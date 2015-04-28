/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.logging;


import com.gemstone.org.jgroups.util.StringId;

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
 * @author Mitch Thomas
 * @since 5.0
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
   * @see com.gemstone.org.jgroups.util.StringId
   * @since 6.0 
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
