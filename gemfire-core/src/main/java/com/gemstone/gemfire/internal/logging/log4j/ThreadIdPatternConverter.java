package com.gemstone.gemfire.internal.logging.log4j;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;

/**
 * Formats the event thread id.
 */
@Plugin(name = "ThreadIdPatternConverter", category = "Converter")
@ConverterKeys({ "tid", "threadId" })
public final class ThreadIdPatternConverter extends LogEventPatternConverter {
  /**
   * Singleton.
   */
  private static final ThreadIdPatternConverter INSTANCE =
      new ThreadIdPatternConverter();

  /**
   * Private constructor.
   */
  private ThreadIdPatternConverter() {
      super("ThreadId", "threadId");
  }

  /**
   * Obtains an instance of ThreadPatternConverter.
   *
   * @param options options, currently ignored, may be null.
   * @return instance of ThreadPatternConverter.
   */
  public static ThreadIdPatternConverter newInstance(
      final String[] options) {
      return INSTANCE;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void format(final LogEvent event, final StringBuilder toAppendTo) {
      toAppendTo.append("0x").append(Long.toHexString(Thread.currentThread().getId()));
  }
}
