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
package org.apache.geode.security;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Properties;

import org.apache.geode.pdx.SimpleClass;
import org.apache.geode.pdx.internal.PdxInstanceImpl;

public class PDXPostProcessor implements PostProcessor {

  private static final byte[] BYTES = {1, 0};

  static byte[] bytes() {
    return BYTES;
  }

  private boolean pdx = false;
  private int count = 0;

  @Override
  public void init(Properties securityProps) {
    this.pdx = Boolean.parseBoolean(securityProps.getProperty("security-pdx"));
    this.count = 0;
  }

  @Override
  public Object processRegionValue(final Object principal, final String regionName,
      final Object key, final Object value) {
    this.count++;
    if (value instanceof byte[]) {
      assertThat(Arrays.equals(BYTES, (byte[]) value)).isTrue();
    } else if (this.pdx) {
      assertThat(value).isInstanceOf(PdxInstanceImpl.class);
    } else {
      assertThat(value).isInstanceOf(SimpleClass.class);
    }
    return value;
  }

  public int getCount() {
    return this.count;
  }
}
