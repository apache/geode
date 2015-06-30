/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.statistics;

import java.util.TimeZone;

import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.StatArchiveWriter;

/**
 * @author Kirk Lund
 * @since 7.0
 */
public class TestStatArchiveWriter extends StatArchiveWriter {
  public static final long WRITER_PREVIOUS_TIMESTAMP_NANOS = 432662613L;
  public static final long WRITER_INITIAL_DATE_MILLIS = 1340037741173L;
  public static final TimeZone WRITER_TIME_ZONE = TimeZone.getTimeZone("PDT");
  public static final String WRITER_OS_INFO = "Linux 2.6.18-262.el5";
  public static final String WRITER_MACHINE_INFO = "i386 kuwait";
  
  public TestStatArchiveWriter(StatArchiveDescriptor archiveDescriptor, LogWriterI18n logger) {
    super(archiveDescriptor);
    initialize(WRITER_PREVIOUS_TIMESTAMP_NANOS);
  }

  @Override
  protected long initInitialDate() {
    return WRITER_INITIAL_DATE_MILLIS;
  }

  @Override
  protected TimeZone getTimeZone() {
    return WRITER_TIME_ZONE;
  }
  
  @Override
  protected String getOSInfo() {
    return WRITER_OS_INFO;
  }
  
  @Override
  protected String getMachineInfo() {
    return WRITER_MACHINE_INFO;
  }
}
