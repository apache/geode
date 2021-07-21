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
package org.apache.geode.internal.cache.tx;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.geode.cache.Operation;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.EntryEventImpl.OldValueImporter;
import org.apache.geode.internal.cache.OldValueImporterTestBase;
import org.apache.geode.internal.cache.tx.RemotePutMessage.PutReplyMessage;

public class RemotePutReplyMessageJUnitTest extends OldValueImporterTestBase {

  @Override
  protected OldValueImporter createImporter() {
    return new PutReplyMessage(1, true, Operation.PUT_IF_ABSENT, null, null, null);
  }

  @Override
  protected Object getOldValueFromImporter(OldValueImporter ovi) {
    return ((PutReplyMessage) ovi).getOldValue();
  }

  @Override
  protected void toData(OldValueImporter ovi, HeapDataOutputStream hdos) throws IOException {
    ((PutReplyMessage) ovi).toData(hdos, InternalDataSerializer.createSerializationContext(hdos));
  }

  @Override
  protected void fromData(OldValueImporter ovi, byte[] bytes)
      throws IOException, ClassNotFoundException {
    final DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));
    ((PutReplyMessage) ovi).fromData(dataInputStream,
        InternalDataSerializer.createDeserializationContext(dataInputStream));
  }
}
