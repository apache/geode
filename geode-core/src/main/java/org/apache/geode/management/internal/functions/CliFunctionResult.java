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

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;

public class CliFunctionResult implements Comparable<CliFunctionResult>, DataSerializableFixedID {

  static {
    InternalDataSerializer.getDSFIDSerializer().registerDSFID(CLI_FUNCTION_RESULT,
        CliFunctionResult.class);

  }

  private String memberIdOrName;
  private Serializable[] serializables = new String[0];
  private Object resultObject;
  private XmlEntity xmlEntity;
  private byte[] byteData = new byte[0];
  private StatusState state;

  public enum StatusState {
    OK, ERROR, IGNORABLE
  }

  @Deprecated
  public CliFunctionResult() {}

  @Deprecated
  public CliFunctionResult(final String memberIdOrName) {
    this.memberIdOrName = memberIdOrName;
    this.state = StatusState.OK;
  }

  @Deprecated
  public CliFunctionResult(final String memberIdOrName, final Serializable[] serializables) {
    this.memberIdOrName = memberIdOrName;
    this.serializables = serializables;
    this.state = StatusState.OK;
  }

  @Deprecated
  public CliFunctionResult(final String memberIdOrName, final XmlEntity xmlEntity) {
    this.memberIdOrName = memberIdOrName;
    this.xmlEntity = xmlEntity;
    this.state = StatusState.OK;
  }

  @Deprecated
  public CliFunctionResult(final String memberIdOrName, final XmlEntity xmlEntity,
      final Serializable[] serializables) {
    this.memberIdOrName = memberIdOrName;
    this.xmlEntity = xmlEntity;
    this.serializables = serializables;
    this.state = StatusState.OK;
  }

  @Deprecated
  public CliFunctionResult(final String memberIdOrName, XmlEntity xmlEntity, final String message) {
    this.memberIdOrName = memberIdOrName;
    this.xmlEntity = xmlEntity;
    if (message != null) {
      this.serializables = new String[] {message};
    }
    this.state = StatusState.OK;
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
      this.serializables = new String[] {message};
    }
  }

  public CliFunctionResult(final String memberIdOrName, final Object resultObject,
      final String message) {
    this.memberIdOrName = memberIdOrName;
    this.resultObject = resultObject;
    if (message != null) {
      this.serializables = new String[] {message};
    }
    if (resultObject instanceof Throwable) {
      this.state = StatusState.ERROR;
    } else {
      this.state = StatusState.OK;
    }
  }

  public CliFunctionResult(final String memberIdOrName, final Object resultObject) {
    this(memberIdOrName, resultObject, null);
  }

  public String getMemberIdOrName() {
    return this.memberIdOrName;
  }

  @Deprecated
  public String getMessage() {
    if (this.serializables.length == 0 || !(this.serializables[0] instanceof String)) {
      return null;
    }

    return (String) this.serializables[0];
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
    return this.serializables;
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
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    toDataPre_GEODE_1_6_0_0(out, context);
    DataSerializer.writeEnum(this.state, out);
  }

  public void toDataPre_GEODE_1_6_0_0(DataOutput out, SerializationContext context)
      throws IOException {
    DataSerializer.writeString(this.memberIdOrName, out);
    DataSerializer.writePrimitiveBoolean(this.isSuccessful(), out);
    context.getSerializer().writeObject(this.xmlEntity, out);
    DataSerializer.writeObjectArray(this.serializables, out);
    context.getSerializer().writeObject(this.resultObject, out);
    DataSerializer.writeByteArray(this.byteData, out);
  }

  public void toDataPre_GFE_8_0_0_0(DataOutput out, SerializationContext context)
      throws IOException {
    DataSerializer.writeString(this.memberIdOrName, out);
    DataSerializer.writeObjectArray(this.serializables, out);
    context.getSerializer().writeObject(this.resultObject, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    fromDataPre_GEODE_1_6_0_0(in, context);
    this.state = DataSerializer.readEnum(StatusState.class, in);
  }

  public void fromDataPre_GEODE_1_6_0_0(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    this.memberIdOrName = DataSerializer.readString(in);
    this.state = DataSerializer.readPrimitiveBoolean(in) ? StatusState.OK : StatusState.ERROR;
    this.xmlEntity = context.getDeserializer().readObject(in);
    this.serializables = (Serializable[]) DataSerializer.readObjectArray(in);
    this.resultObject = context.getDeserializer().readObject(in);
    this.byteData = DataSerializer.readByteArray(in);
  }

  public void fromDataPre_GFE_8_0_0_0(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    this.memberIdOrName = DataSerializer.readString(in);
    this.resultObject = context.getDeserializer().readObject(in);
    this.serializables = (Serializable[]) DataSerializer.readObjectArray(in);
  }

  public boolean isSuccessful() {
    return this.state == StatusState.OK;
  }

  public boolean isIgnorableFailure() {
    return this.state == StatusState.IGNORABLE;
  }

  @Deprecated
  public XmlEntity getXmlEntity() {
    return this.xmlEntity;
  }

  @Deprecated
  public byte[] getByteData() {
    return this.byteData;
  }

  @Override
  public int compareTo(CliFunctionResult o) {
    if (this.memberIdOrName == null && o.memberIdOrName == null) {
      return 0;
    }
    if (this.memberIdOrName == null && o.memberIdOrName != null) {
      return -1;
    }
    if (this.memberIdOrName != null && o.memberIdOrName == null) {
      return 1;
    }
    return getMemberIdOrName().compareTo(o.memberIdOrName);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((this.memberIdOrName == null) ? 0 : this.memberIdOrName.hashCode());
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
    if (this.memberIdOrName == null) {
      if (other.memberIdOrName != null) {
        return false;
      }
    } else if (!this.memberIdOrName.equals(other.memberIdOrName)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "CliFunctionResult [memberId=" + this.memberIdOrName + ", successful="
        + this.isSuccessful() + ", xmlEntity=" + this.xmlEntity + ", serializables="
        + Arrays.toString(this.serializables) + ", throwable=" + this.resultObject + ", byteData="
        + Arrays.toString(this.byteData) + "]";
  }

  /**
   * Remove elements from the list that are not instances of CliFunctionResult and then sort the
   * results.
   *
   * @param results The results to clean.
   * @return The cleaned results.
   */
  public static List<CliFunctionResult> cleanResults(List<?> results) {
    List<CliFunctionResult> returnResults = new ArrayList<CliFunctionResult>(results.size());
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
    return new KnownVersion[] {KnownVersion.GFE_80};
  }
}
