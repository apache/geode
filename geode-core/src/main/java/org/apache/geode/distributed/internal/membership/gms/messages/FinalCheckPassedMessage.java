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
package org.apache.geode.distributed.internal.membership.gms.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.distributed.internal.membership.gms.api.MemberIdentifier;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.Version;

public class FinalCheckPassedMessage<ID extends MemberIdentifier> extends AbstractGMSMessage<ID> {

  private ID suspect;

  public FinalCheckPassedMessage() {}

  public FinalCheckPassedMessage(ID recipient, ID suspect) {
    super();
    setRecipient(recipient);
    this.suspect = suspect;
  }

  @Override
  public int getDSFID() {
    return FINAL_CHECK_PASSED_MESSAGE;
  }

  @Override
  public String toString() {
    return "FinalCheckPassedMessage [suspect=" + suspect + "]";
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    context.getSerializer().writeObject(suspect, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    suspect = context.getDeserializer().readObject(in);
  }

  public ID getSuspect() {
    return suspect;
  }
}
