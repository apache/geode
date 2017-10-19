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

import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.management.internal.cli.functions.RegionFunctionArgs.ExpirationAttrs;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class ExpirationAttrsTest {

  private static ExpirationAttrs.ExpirationFor expirationFor =
      ExpirationAttrs.ExpirationFor.ENTRY_IDLE;

  @Test
  public void constructor() throws Exception {
    ExpirationAttrs attrs = new ExpirationAttrs(expirationFor, null, null);
    assertThat(attrs.getTime()).isEqualTo(0);
    assertThat(attrs.getAction()).isEqualTo(ExpirationAction.INVALIDATE);

    attrs = new ExpirationAttrs(expirationFor, -1, null);
    assertThat(attrs.getTime()).isEqualTo(0);
    assertThat(attrs.getAction()).isEqualTo(ExpirationAction.INVALIDATE);

    attrs = new ExpirationAttrs(expirationFor, 0, null);
    assertThat(attrs.getTime()).isEqualTo(0);
    assertThat(attrs.getAction()).isEqualTo(ExpirationAction.INVALIDATE);

    attrs = new ExpirationAttrs(expirationFor, 2, "destroy");
    assertThat(attrs.getTime()).isEqualTo(2);
    assertThat(attrs.getAction()).isEqualTo(ExpirationAction.DESTROY);
  }
}
