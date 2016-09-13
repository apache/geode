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
   
   
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.*;
import com.gemstone.gemfire.admin.GemFireHealth;
import java.io.*;

/**
 * A message that is sent to a particular distribution manager to
 * fetch its health diagnosis
 * @since GemFire 3.5
 */
public final class FetchHealthDiagnosisRequest extends AdminRequest {
  // instance variables
  private int id = 0;

  private GemFireHealth.Health healthCode = null;

  /**
   * Returns a <code>FetchHealthDiagnosisRequest</code> to be sent to the specified recipient.
   */
  public static FetchHealthDiagnosisRequest create(int id, GemFireHealth.Health healthCode) {
    FetchHealthDiagnosisRequest m = new FetchHealthDiagnosisRequest();
    m.init_( id, healthCode );
    return m;
  }

  public FetchHealthDiagnosisRequest() {
    init_( 0, null );
  }

  /**
   * Must return a proper response to this request.
   */
  @Override  
  protected AdminResponse createResponse(DistributionManager dm) {
    return FetchHealthDiagnosisResponse.create(dm, this.getSender(),
                                               this.id, this.healthCode);
  }

  public int getDSFID() {
    return FETCH_HEALTH_DIAGNOSIS_REQUEST;
  }

  @Override  
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.id);
    DataSerializer.writeObject(this.healthCode, out);
  }

  @Override  
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    int i = in.readInt();
    GemFireHealth.Health oHC = (GemFireHealth.Health) DataSerializer.readObject(in);
    init_( i, oHC );
  }

  @Override  
  public String toString() {
    return LocalizedStrings.FetchHealthDiagnosisRequest_FETCHHEALTHDIAGNOSISREQUEST_FROM_ID_1_HEALTHCODE_2.toLocalizedString(new Object[] {this.getRecipient(), Integer.valueOf(this.id), this.healthCode});
  }


  private void init_( int i, GemFireHealth.Health oHC ) {
    this.id = i;
    this.healthCode = oHC;
    this.friendlyName = LocalizedStrings.FetchHealthDiagnosisRequest_FETCH_HEALTH_DIAGNOSIS_FOR_HEALTH_CODE_0.toLocalizedString(this.healthCode);
  }
}
