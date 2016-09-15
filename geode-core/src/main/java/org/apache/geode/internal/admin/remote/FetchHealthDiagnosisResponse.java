/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
   
   
package org.apache.geode.internal.admin.remote;

import org.apache.geode.*;
import org.apache.geode.admin.GemFireHealth;
//import org.apache.geode.internal.*;
//import org.apache.geode.internal.admin.*;
import org.apache.geode.distributed.internal.*;
import java.io.*;
//import java.util.*;
import org.apache.geode.distributed.internal.membership.*;

/**
 * The response to fetching the health diagnosis.
 * @since GemFire 3.5
 */
public final class FetchHealthDiagnosisResponse extends AdminResponse {
  // instance variables
  String[] diagnosis;
  
  /**
   * Returns a <code>FetchHealthDiagnosisResponse</code> that will be returned to the
   * specified recipient.
   */
  public static FetchHealthDiagnosisResponse create(DistributionManager dm, InternalDistributedMember recipient, int id,
                                                    GemFireHealth.Health healthCode) {
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
    return this.diagnosis;
  }

  // instance methods
  public int getDSFID() {
    return FETCH_HEALTH_DIAGNOSIS_RESPONSE;
  }

  @Override  
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeStringArray(this.diagnosis, out);
  }

  @Override  
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.diagnosis = DataSerializer.readStringArray(in);
  }

  @Override  
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("FetchHealthDiagnosisResponse from ");
    sb.append(this.getRecipient());
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
