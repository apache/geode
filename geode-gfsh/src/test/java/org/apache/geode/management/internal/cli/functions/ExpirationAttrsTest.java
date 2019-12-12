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

package org.apache.geode.management.internal.cli.functions;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.management.internal.cli.functions.RegionFunctionArgs.ExpirationAttrs;

public class ExpirationAttrsTest {

  private ExpirationAttrs attrs;
  private ExpirationAttributes existing;

  @Before
  public void setUp() throws Exception {
    existing = new ExpirationAttributes(5, ExpirationAction.DESTROY);
  }

  @Test
  public void emptyConstructor() {
    attrs = new ExpirationAttrs(null, null);
    assertThat(attrs.getTime()).isNull();
    assertThat(attrs.getAction()).isNull();

    assertThat(attrs.isTimeSet()).isFalse();
    assertThat(attrs.isTimeOrActionSet()).isFalse();

    assertThat(attrs.getExpirationAttributes()).isEqualTo(new ExpirationAttributes());
    assertThat(attrs.getExpirationAttributes(existing)).isEqualTo(existing);
  }

  @Test
  public void constructorWithAction() {
    attrs = new ExpirationAttrs(null, ExpirationAction.LOCAL_DESTROY);
    assertThat(attrs.getTime()).isNull();
    assertThat(attrs.getAction()).isEqualTo(ExpirationAction.LOCAL_DESTROY);

    assertThat(attrs.isTimeSet()).isFalse();
    assertThat(attrs.isTimeOrActionSet()).isTrue();

    assertThat(attrs.getExpirationAttributes())
        .isEqualTo(new ExpirationAttributes(0, ExpirationAction.LOCAL_DESTROY));
    assertThat(attrs.getExpirationAttributes(existing))
        .isEqualTo(new ExpirationAttributes(5, ExpirationAction.LOCAL_DESTROY));
  }

  @Test
  public void constructorWithTime() {
    attrs = new ExpirationAttrs(10, null);
    assertThat(attrs.getTime()).isEqualTo(10);
    assertThat(attrs.getAction()).isNull();

    assertThat(attrs.isTimeSet()).isTrue();
    assertThat(attrs.isTimeOrActionSet()).isTrue();

    assertThat(attrs.getExpirationAttributes())
        .isEqualTo(new ExpirationAttributes(10, ExpirationAction.INVALIDATE));
    assertThat(attrs.getExpirationAttributes(existing))
        .isEqualTo(new ExpirationAttributes(10, ExpirationAction.DESTROY));
  }
}
