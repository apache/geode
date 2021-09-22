/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.functions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.DSFIDLoader;
import org.apache.geode.internal.serialization.DSFIDSerializer;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;

public class CliFunctionResult implements Comparable<CliFunctionResult>, DataSerializableFixedID,
    DSFIDLoader {

  @Immutable
  private static final KnownVersion[] KNOWN_VERSIONS = {KnownVersion.GEODE_1_6_0};

  private String memberIdOrName;
  private Serializable[] serializables = new String[0];
  private Object resultObject;
  private XmlEntity xmlEntity;
  private byte[] byteData = new byte[0];
  private StatusState state;

  public enum StatusState {
    OK, ERROR, IGNORABLE;
  }

  @Deprecated
  public CliFunctionResult() {}

  @Deprecated
  public CliFunctionResult(final String memberIdOrName) {
    this.memberIdOrName = memberIdOrName;
    state = StatusState.OK;
  }

  @Deprecated
  public CliFunctionResult(final String memberIdOrName, final Serializable[] serializables) {
    this.memberIdOrName = memberIdOrName;
    this.serializables = serializables;
    state = StatusState.OK;
  }

  @Deprecated
  public CliFunctionResult(final String memberIdOrName, final XmlEntity xmlEntity) {
    this.memberIdOrName = memberIdOrName;
    this.xmlEntity = xmlEntity;
    state = StatusState.OK;
  }

  @Deprecated
  public CliFunctionResult(final String memberIdOrName, final XmlEntity xmlEntity,
      final Serializable[] serializables) {
    this.memberIdOrName = memberIdOrName;
    this.xmlEntity = xmlEntity;
    this.serializables = serializables;
    state = StatusState.OK;
  }

  @Deprecated
  public CliFunctionResult(final String memberIdOrName, XmlEntity xmlEntity, final String message) {
    this.memberIdOrName = memberIdOrName;
    this.xmlEntity = xmlEntity;
    if (message != null) {
      serializables = new String[] {message};
    }
    state = StatusState.OK;
  }

  public CliFunctionResult(final String memberIdOrName, final boolean successful,
      final String message) {
    this(memberIdOrName, successful ? StatusState.OK : StatusState.ERROR, message);
  }

  public CliFunctionResult(final String memberIdOrName, final StatusState state,
      final String message) {
    this.memberIdOrName = memberIdOrName;
    this.state = state;
    if (message != null) {
      serializables = new String[] {message};
    }
  }

  public CliFunctionResult(final String memberIdOrName, final Object resultObject,
      final String message) {
    this.memberIdOrName = memberIdOrName;
    this.resultObject = resultObject;
    if (message != null) {
      serializables = new String[] {message};
    }
    if (resultObject instanceof Throwable) {
      state = StatusState.ERROR;
    } else {
      state = StatusState.OK;
    }
  }

  public CliFunctionResult(final String memberIdOrName, final Object resultObject) {
    this(memberIdOrName, resultObject, null);
  }

  public String getMemberIdOrName() {
    return memberIdOrName;
  }

  @Deprecated
  public String getMessage() {
    if (serializables.length == 0 || !(serializables[0] instanceof String)) {
      return null;
    }

    return (String) serializables[0];
  }

  public String getStatus(boolean skipIgnore) {
    if (state == StatusState.IGNORABLE) {
      return skipIgnore ? "IGNORED" : "ERROR";
    }

    return state.name();
  }

  public String getStatus() {
    return getStatus(true);
  }

  public String getStatusMessage() {
    String message = getMessage();

    if (isSuccessful()) {
      return message;
    }

    String errorMessage = "";
    if (message != null
        && (resultObject == null || !((Throwable) resultObject).getMessage().contains(message))) {
      errorMessage = message;
    }

    if (resultObject != null) {
      errorMessage = errorMessage.trim() + " " + ((Throwable) resultObject).getClass().getName()
          + ": " + ((Throwable) resultObject).getMessage();
    }

    return errorMessage;
  }

  /**
   * This can be removed once all commands are using ResultModel.
   */
  @Deprecated
  public String getLegacyStatus() {
    String message = getMessage();

    if (isSuccessful()) {
      return message;
    }

    String errorMessage = "ERROR: ";
    if (message != null
        && (resultObject == null || !((Throwable) resultObject).getMessage().contains(message))) {
      errorMessage += message;
    }

    if (resultObject != null) {
      errorMessage = errorMessage.trim() + " " + ((Throwable) resultObject).getClass().getName()
          + ": " + ((Throwable) resultObject).getMessage();
    }

    return errorMessage;
  }

  @Deprecated
  public Serializable[] getSerializables() {
    return serializables;
  }

  @Deprecated
  public Throwable getThrowable() {
    if (isSuccessful()) {
      return null;
    }
    return ((Throwable) resultObject);
  }

  public Object getResultObject() {
    return resultObject;
  }

  @Override
  public int getDSFID() {
    return DataSerializableFixedID.CLI_FUNCTION_RESULT;
  }

  @Override
  public void registerDSFIDs(DSFIDSerializer serializer) {
    serializer.registerDSFID(CLI_FUNCTION_RESULT, CliFunctionResult.class);
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    toDataPre_GEODE_1_6_0_0(out, context);
    DataSerializer.writeEnum(state, out);
  }

  public void toDataPre_GEODE_1_6_0_0(DataOutput out, SerializationContext context)
      throws IOException {
    DataSerializer.writeString(memberIdOrName, out);
    DataSerializer.writePrimitiveBoolean(isSuccessful(), out);
    context.getSerializer().writeObject(xmlEntity, out);
    DataSerializer.writeObjectArray(serializables, out);
    context.getSerializer().writeObject(resultObject, out);
    DataSerializer.writeByteArray(byteData, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    fromDataPre_GEODE_1_6_0_0(in, context);
    state = DataSerializer.readEnum(StatusState.class, in);
  }

  public void fromDataPre_GEODE_1_6_0_0(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    memberIdOrName = DataSerializer.readString(in);
    state = DataSerializer.readPrimitiveBoolean(in) ? StatusState.OK : StatusState.ERROR;
    xmlEntity = context.getDeserializer().readObject(in);
    serializables = (Serializable[]) DataSerializer.readObjectArray(in);
    resultObject = context.getDeserializer().readObject(in);
    byteData = DataSerializer.readByteArray(in);
  }

  public boolean isSuccessful() {
    return state == StatusState.OK;
  }

  public boolean isIgnorableFailure() {
    return state == StatusState.IGNORABLE;
  }

  @Deprecated
  public XmlEntity getXmlEntity() {
    return xmlEntity;
  }

  @Deprecated
  public byte[] getByteData() {
    return byteData;
  }

  @Override
  public int compareTo(@NotNull CliFunctionResult o) {
    if (memberIdOrName == null && o.memberIdOrName == null) {
      return 0;
    }
    if (memberIdOrName == null) {
      return -1;
    }
    if (o.memberIdOrName == null) {
      return 1;
    }

    return memberIdOrName.compareTo(o.memberIdOrName);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((memberIdOrName == null) ? 0 : memberIdOrName.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    CliFunctionResult other = (CliFunctionResult) obj;
    if (memberIdOrName == null) {
      return other.memberIdOrName == null;
    } else {
      return memberIdOrName.equals(other.memberIdOrName);
    }
  }

  @Override
  public String toString() {
    return "CliFunctionResult [memberId=" + memberIdOrName + ", successful="
        + isSuccessful() + ", xmlEntity=" + xmlEntity + ", serializables="
        + Arrays.toString(serializables) + ", throwable=" + resultObject + ", byteData="
        + Arrays.toString(byteData) + "]";
  }

  /**
   * Remove elements from the list that are not instances of CliFunctionResult and then sort the
   * results.
   *
   * @param results The results to clean.
   * @return The cleaned results.
   */
  public static List<CliFunctionResult> cleanResults(List<?> results) {
    List<CliFunctionResult> returnResults = new ArrayList<>(results.size());
    for (Object result : results) {
      if (result instanceof CliFunctionResult) {
        returnResults.add((CliFunctionResult) result);
      }
    }

    Collections.sort(returnResults);
    return returnResults;
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return KNOWN_VERSIONS;
  }
}
