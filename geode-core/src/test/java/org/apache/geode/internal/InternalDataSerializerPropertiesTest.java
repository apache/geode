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

package org.apache.geode.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import org.junit.Test;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.serialization.ByteArrayDataInput;

public class InternalDataSerializerPropertiesTest {

  @Test
  public void readStringPropertiesReadsStringAndByteArrayValues() throws Exception {
    final Properties written = new Properties();
    written.put("user", "alice");
    written.put("token", new byte[] {1, 2, 3});
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataSerializer.writeProperties(written, new DataOutputStream(bytes));

    final Properties read =
        InternalDataSerializer.readStringProperties(new ByteArrayDataInput(bytes.toByteArray()));

    assertThat(read.get("user")).isEqualTo("alice");
    assertThat((byte[]) read.get("token")).containsExactly(1, 2, 3);
  }

  @Test
  public void readStringPropertiesRejectsOtherValueTypes() throws Exception {
    ValueProbe.deserialized = false;
    final Properties written = new Properties();
    written.put("user", new ValueProbe());
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataSerializer.writeProperties(written, new DataOutputStream(bytes));

    assertThatThrownBy(() -> InternalDataSerializer
        .readStringProperties(new ByteArrayDataInput(bytes.toByteArray())))
            .isInstanceOf(IOException.class);
    assertThat(ValueProbe.deserialized).isFalse();
  }

  public static class ValueProbe implements Serializable {
    static boolean deserialized;

    private void readObject(java.io.ObjectInputStream in)
        throws IOException, ClassNotFoundException {
      in.defaultReadObject();
      deserialized = true;
    }
  }
}
