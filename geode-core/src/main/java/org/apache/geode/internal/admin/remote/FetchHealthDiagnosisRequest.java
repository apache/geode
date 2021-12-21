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


package org.apache.geode.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.admin.GemFireHealth;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A message that is sent to a particular distribution manager to fetch its health diagnosis
 *
 * @since GemFire 3.5
 */
public class FetchHealthDiagnosisRequest extends AdminRequest {
  // instance variables
  private int id = 0;

  private GemFireHealth.Health healthCode = null;

  /**
   * Returns a <code>FetchHealthDiagnosisRequest</code> to be sent to the specified recipient.
   */
  public static FetchHealthDiagnosisRequest create(int id, GemFireHealth.Health healthCode) {
    FetchHealthDiagnosisRequest m = new FetchHealthDiagnosisRequest();
    m.init_(id, healthCode);
    return m;
  }

  public FetchHealthDiagnosisRequest() {
    init_(0, null);
  }

  /**
   * Must return a proper response to this request.
   */
  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    return FetchHealthDiagnosisResponse.create(dm, getSender(), id, healthCode);
  }

  @Override
  public int getDSFID() {
    return FETCH_HEALTH_DIAGNOSIS_REQUEST;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeInt(id);
    DataSerializer.writeObject(healthCode, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    int i = in.readInt();
    GemFireHealth.Health oHC = DataSerializer.readObject(in);
    init_(i, oHC);
  }

  @Override
  public String toString() {
    return String.format("FetchHealthDiagnosisRequest from %s id=%s healthCode=%s",
        getRecipient(), Integer.valueOf(id), healthCode);
  }


  private void init_(int i, GemFireHealth.Health oHC) {
    id = i;
    healthCode = oHC;
    friendlyName =
        String.format("fetch health diagnosis for health code %s",
            healthCode);
  }
}
