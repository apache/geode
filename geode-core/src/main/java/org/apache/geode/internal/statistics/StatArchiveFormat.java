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

/**
 * StatArchiveFormat defines constants related to the statistic archive file format.
 */
public interface StatArchiveFormat {
  /**
   * ARCHIVE_VERSION identifies the format of the contents of the archive. It should be changed any
   * time an incompatible change is made. Its current value is <code>3</code>.
   * <p>
   * <ol>
   * <li>Changed from 2 to 3 with addition of <code>RESOURCE_INSTANCE_INITIALIZE_TOKEN</code>
   * <li>Changed from 3 to 4 with addition of largerBetter boolean in statistic descriptor
   * </ol>
   */
  byte ARCHIVE_VERSION = 4;

  /**
   * Token consists of a timestamp and any statistic value changes.
   */
  byte SAMPLE_TOKEN = 0;
  /**
   * Token defines a new resource type.
   */
  byte RESOURCE_TYPE_TOKEN = 1;
  /**
   * Token defines a new resource instance.
   */
  byte RESOURCE_INSTANCE_CREATE_TOKEN = 2;
  /**
   * Token notes that a previous resource instance no longer exists and thus will have any more
   * samples of its statistic values taken.
   */
  byte RESOURCE_INSTANCE_DELETE_TOKEN = 3;
  /**
   * Token defines a new resource instance with initial data.
   */
  byte RESOURCE_INSTANCE_INITIALIZE_TOKEN = 4;
  /**
   * Token defines a new archive and provides some global information about the environment the
   * archive was created in.
   */
  byte HEADER_TOKEN = 77;

  /**
   * The value used to signal the end of a list of resource instances.
   */
  int ILLEGAL_RESOURCE_INST_ID = -1;
  /**
   * The maximum value a resource inst id can have and still be stored in the archive as an unsigned
   * byte.
   */
  int MAX_BYTE_RESOURCE_INST_ID = 252;
  /**
   * Used to say that the next two bytes contain the resource inst id as an unsigned short.
   */
  int SHORT_RESOURCE_INST_ID_TOKEN = 253;
  /**
   * Used to say that the next four bytes contain the resource inst id as an int.
   */
  int INT_RESOURCE_INST_ID_TOKEN = 254;
  /**
   * Used to say that the current byte represents the ILLEGAL_RESOURCE_INST_ID.
   */
  int ILLEGAL_RESOURCE_INST_ID_TOKEN = 255;
  /**
   * The maximum value a resource inst id can have and still be stored in the archive as an unsigned
   * short.
   */
  int MAX_SHORT_RESOURCE_INST_ID = 65535;
  /**
   * The value used to signal the end of a list of statistic samples.
   */
  int ILLEGAL_STAT_OFFSET = 255;

  /**
   * The maximum value a timestamp can have and still be stored in the archive as an unsigned short.
   */
  int MAX_SHORT_TIMESTAMP = 65534;
  /**
   * Means the next 4 bytes contain the timestamp as an int.
   */
  int INT_TIMESTAMP_TOKEN = 65535;

  /**
   * The maximum value a compact value can have and still be stored in the archive as one byte.
   */
  int MAX_1BYTE_COMPACT_VALUE = Byte.MAX_VALUE;
  /**
   * The minimum value a compact value can have and still be stored in the archive as one byte.
   */
  int MIN_1BYTE_COMPACT_VALUE = Byte.MIN_VALUE + 7;
  /**
   * The maximum value a compact value can have and still be stored in the archive as two bytes.
   */
  int MAX_2BYTE_COMPACT_VALUE = Short.MAX_VALUE;
  /**
   * The minimum value a compact value can have and still be stored in the archive as two bytes.
   */
  int MIN_2BYTE_COMPACT_VALUE = Short.MIN_VALUE;

  /**
   * Means the next 2 bytes hold the compact value's data.
   */
  int COMPACT_VALUE_2_TOKEN = Byte.MIN_VALUE;
  /**
   * Means the next 3 bytes hold the compact value's data.
   */
  int COMPACT_VALUE_3_TOKEN = Byte.MIN_VALUE + 1;
  /**
   * Means the next 4 bytes hold the compact value's data.
   */
  int COMPACT_VALUE_4_TOKEN = Byte.MIN_VALUE + 2;
  /**
   * Means the next 5 bytes hold the compact value's data.
   */
  int COMPACT_VALUE_5_TOKEN = Byte.MIN_VALUE + 3;
  /**
   * Means the next 6 bytes hold the compact value's data.
   */
  int COMPACT_VALUE_6_TOKEN = Byte.MIN_VALUE + 4;
  /**
   * Means the next 7 bytes hold the compact value's data.
   */
  int COMPACT_VALUE_7_TOKEN = Byte.MIN_VALUE + 5;
  /**
   * Means the next 8 bytes hold the compact value's data.
   */
  int COMPACT_VALUE_8_TOKEN = Byte.MIN_VALUE + 6;

  /**
   * Statistic represents a <code>boolean</code> java primitive.
   */
  int BOOLEAN_CODE = 1;
  /**
   * Statistic represents a <code>char</code> java primitive.
   */
  int CHAR_CODE = 2;
  /**
   * Statistic represents a <code>char</code> java primitive.
   */
  int WCHAR_CODE = 12;
  /**
   * Statistic represents a <code>byte</code> java primitive.
   */
  int BYTE_CODE = 3;
  /**
   * Statistic represents a <code>short</code> java primitive.
   */
  int SHORT_CODE = 4;
  /**
   * Statistic represents a <code>int</code> java primitive.
   */
  int INT_CODE = 5;
  /**
   * Statistic represents a <code>long</code> java primitive.
   */
  int LONG_CODE = 6;
  /**
   * Statistic represents a <code>float</code> java primitive.
   */
  int FLOAT_CODE = 7;
  /**
   * Statistic represents a <code>double</code> java primitive.
   */
  int DOUBLE_CODE = 8;
  /**
   * Number of nanoseconds in one millisecond
   */
  long NANOS_PER_MILLI = 1000000;

}
