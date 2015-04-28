package com.gemstone.gemfire.internal.logging;

/**
 * Prints messages formatted like GemFire log messages to stderr.
 * 
 * @author Kirk Lund
 */
public class StandardErrorPrinter extends LocalLogWriter {

  /**
   * Creates a writer that logs to <code>System.err</code>. All messages will be logged.
   * @throws IllegalArgumentException if level is not in legal range
   */
  public StandardErrorPrinter() {
    this(InternalLogWriter.ALL_LEVEL);
  }

  /**
   * Creates a writer that logs to <code>System.err</code>.
   * @param level only messages greater than or equal to this value will be logged.
   * @throws IllegalArgumentException if level is not in legal range
   */
  public StandardErrorPrinter(final int level) {
    super(level, System.err);
  }

}
