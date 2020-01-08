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
package org.apache.geode.internal.cache;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.internal.serialization.VersionedDataInputStream;

public class EventIDTest {

  @Test
  public void emptyEventIdCanBeSerializedWithCurrentVersion()
      throws IOException, ClassNotFoundException {
    emptyEventIdCanBeSerialized(Version.CURRENT);

  }

  @Test
  public void emptyEventIdCanBeSerializedToGeode100() throws IOException, ClassNotFoundException {
    emptyEventIdCanBeSerialized(Version.GFE_90);
  }

  private void emptyEventIdCanBeSerialized(Version version)
      throws IOException, ClassNotFoundException {
    EventID eventID = new EventID();
    HeapDataOutputStream out = new HeapDataOutputStream(version);
    DataSerializer.writeObject(eventID, out);

    EventID result = DataSerializer.readObject(
        new VersionedDataInputStream(new ByteArrayInputStream(out.toByteArray()), version));

    Assertions.assertThat(result.getMembershipID()).isEqualTo(eventID.getMembershipID());
  }

}
