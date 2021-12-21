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
import org.apache.geode.distributed.internal.HealthMonitor;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * The response to fetching the health diagnosis.
 *
 * @since GemFire 3.5
 */
public class FetchHealthDiagnosisResponse extends AdminResponse {
  // instance variables
  String[] diagnosis;

  /**
   * Returns a <code>FetchHealthDiagnosisResponse</code> that will be returned to the specified
   * recipient.
   */
  public static FetchHealthDiagnosisResponse create(DistributionManager dm,
      InternalDistributedMember recipient, int id, GemFireHealth.Health healthCode) {
    FetchHealthDiagnosisResponse m = new FetchHealthDiagnosisResponse();
    m.setRecipient(recipient);
    {
      HealthMonitor hm = dm.getHealthMonitor(recipient);
      if (hm.getId() == id) {
        m.diagnosis = hm.getDiagnosis(healthCode);
      }
    }
    return m;
  }

  public String[] getDiagnosis() {
    return diagnosis;
  }

  // instance methods
  @Override
  public int getDSFID() {
    return FETCH_HEALTH_DIAGNOSIS_RESPONSE;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeStringArray(diagnosis, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    diagnosis = DataSerializer.readStringArray(in);
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("FetchHealthDiagnosisResponse from ");
    sb.append(getRecipient());
    sb.append(" diagnosis=\"");
    for (int i = 0; i < diagnosis.length; i++) {
      sb.append(diagnosis[i]);
      if (i < diagnosis.length - 1) {
        sb.append(" ");
      }
    }
    sb.append("\"");
    return sb.toString();
  }
}
