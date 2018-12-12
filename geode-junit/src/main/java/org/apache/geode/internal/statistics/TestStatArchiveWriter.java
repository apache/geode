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
package org.apache.geode.internal.statistics;

import java.util.TimeZone;

/**
 * @since GemFire 7.0
 */
public class TestStatArchiveWriter extends StatArchiveWriter {
  public static final long WRITER_PREVIOUS_TIMESTAMP_NANOS = 432662613L;
  public static final long WRITER_INITIAL_DATE_MILLIS = 1340037741173L;
  public static final TimeZone WRITER_TIME_ZONE = TimeZone.getTimeZone("PDT");
  public static final String WRITER_OS_INFO = "Linux 2.6.18-262.el5";
  public static final String WRITER_MACHINE_INFO = "i386 kuwait";

  public TestStatArchiveWriter(final StatArchiveDescriptor archiveDescriptor) {
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
