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
package org.apache.geode.util.internal;

import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

@SuppressWarnings("unchecked")
public class UncheckedUtilsTest {

  @Test
  public void uncheckedCast_rawList_empty() {
    List rawList = new ArrayList();

    List<String> value = uncheckedCast(rawList);

    assertThat(value).isSameAs(rawList);
  }

  @Test
  public void uncheckedCast_rawList_nonEmpty() {
    List rawList = new ArrayList();
    rawList.add("1");
    rawList.add("2");

    List<String> value = uncheckedCast(rawList);

    assertThat(value).isSameAs(rawList);
  }

  @Test
  public void uncheckedCast_rawList_wrongTypes() {
    List rawList = new ArrayList();
    rawList.add(1);
    rawList.add(2);
    List<String> wrongType = uncheckedCast(rawList);

    Throwable thrown = catchThrowable(() -> wrongType.get(0));

    assertThat(thrown).isInstanceOf(ClassCastException.class);
  }
}
