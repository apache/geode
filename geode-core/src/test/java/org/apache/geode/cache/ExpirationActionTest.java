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

package org.apache.geode.cache;


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

public class ExpirationActionTest {

  @Test
  public void toXmlString() throws Exception {
    assertThat(ExpirationAction.LOCAL_DESTROY.toXmlString()).isEqualTo("local-destroy");
    assertThat(ExpirationAction.DESTROY.toXmlString()).isEqualTo("destroy");
    assertThat(ExpirationAction.LOCAL_INVALIDATE.toXmlString()).isEqualTo("local-invalidate");
    assertThat(ExpirationAction.INVALIDATE.toXmlString()).isEqualTo("invalidate");
  }

  @Test
  public void fromXmlString() throws Exception {
    assertThat(ExpirationAction.fromXmlString("local-destroy"))
        .isEqualTo(ExpirationAction.LOCAL_DESTROY);
    assertThat(ExpirationAction.fromXmlString("destroy")).isEqualTo(ExpirationAction.DESTROY);
    assertThat(ExpirationAction.fromXmlString("local-invalidate"))
        .isEqualTo(ExpirationAction.LOCAL_INVALIDATE);
    assertThat(ExpirationAction.fromXmlString("invalidate")).isEqualTo(ExpirationAction.INVALIDATE);
    assertThatThrownBy(() -> ExpirationAction.fromXmlString("invalid"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("invalid expiration action: invalid");
  }
}
