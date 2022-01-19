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
package org.apache.persistence.logging;

/**
 * A Handler exports LogRecords to some destination. It can be configured to ignore log records that
 * are below a given level. It can also a have formatter for formatting the log records before
 * exporting the to the destination.
 */
public abstract class Handler {

  /**
   * The minimum level for this handler. Any records below this level are ignored.
   */
  private Level level;

  /** Used to format the log records */
  private Formatter formatter;

  /**
   * Creates a new <code>Handler</code> with Level.ALL and no formatter.
   */
  protected Handler() {
    level = Level.ALL;
    formatter = null;
  }

  /**
   * Closes this Handler and frees all of its resources
   */
  public abstract void close();

  /**
   * Flushes an buffered output
   */
  public abstract void flush();

  /**
   * Returns the formatter for this handler
   */
  public Formatter getFormatter() {
    return (formatter);
  }

  /**
   * Sets the formatter for this handler
   */
  public void setFormatter(Formatter formatter) {
    this.formatter = formatter;
  }

  /**
   * Returns the level below which this handler ignores
   */
  public Level getLevel() {
    return (level);
  }

  /**
   * Sets the level below which this handler ignores
   */
  public void setLevel(Level level) {
    this.level = level;
  }

  /**
   * Returns <code>true</code> if a log record will be handled by this handler.
   */
  public boolean isLoggable(LogRecord record) {
    return record.getLevel().intValue() >= getLevel().intValue();
  }

  /**
   * Publishes a log record to this handler
   */
  public abstract void publish(LogRecord record);

}
