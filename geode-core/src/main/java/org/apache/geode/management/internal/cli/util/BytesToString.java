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
package org.apache.geode.management.internal.cli.util;

import java.text.DecimalFormat;
import java.text.NumberFormat;

public class BytesToString {
  private static final double BYTES_PER_KB = 1024;
  private static final double BYTES_PER_MB = 1024 * BYTES_PER_KB;
  private static final double BYTES_PER_GB = 1024 * BYTES_PER_MB;
  private static final double BYTES_PER_TB = 1024 * BYTES_PER_GB;

  /**
   * Returns a String that concisely displays the given number of bytes, e.g. "1 KB" or "2.49 MB".
   */
  public String of(long sizeInBytes) {
    final NumberFormat numberFormat = new DecimalFormat();
    numberFormat.setMaximumFractionDigits(2);

    try {
      if (sizeInBytes < BYTES_PER_KB) {
        return numberFormat.format(sizeInBytes) + " Byte(s)";
      } else if (sizeInBytes < BYTES_PER_MB) {
        return numberFormat.format(sizeInBytes / BYTES_PER_KB) + " KB";
      } else if (sizeInBytes < BYTES_PER_GB) {
        return numberFormat.format(sizeInBytes / BYTES_PER_MB) + " MB";
      } else if (sizeInBytes < BYTES_PER_TB) {
        return numberFormat.format(sizeInBytes / BYTES_PER_GB) + " GB";
      } else {
        return numberFormat.format(sizeInBytes / BYTES_PER_TB) + " TB";
      }
    } catch (Exception e) {
      return sizeInBytes + " Byte(s)";
    }
  }

}
