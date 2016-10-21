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

import org.apache.geode.distributed.internal.*;
import org.apache.geode.internal.i18n.LocalizedStrings;
// import org.apache.geode.*;
// import org.apache.geode.internal.*;
import java.io.*;
// import java.util.*;

/**
 * A message that is sent to a particular distribution manager to get its current license info.
 */
public final class LicenseInfoRequest extends AdminRequest {
  /**
   * Returns a <code>LicenseInfoRequest</code>.
   */
  public static LicenseInfoRequest create() {
    LicenseInfoRequest m = new LicenseInfoRequest();
    return m;
  }

  public LicenseInfoRequest() {
    friendlyName =
        LocalizedStrings.LicenseInfoRequest_FETCH_CURRENT_LICENSE_INFORMATION.toLocalizedString();
  }

  /**
   * Must return a proper response to this request.
   */
  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    return LicenseInfoResponse.create(dm, this.getSender());
  }

  public int getDSFID() {
    return LICENSE_INFO_REQUEST;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
  }

  @Override
  public String toString() {
    return "LicenseInfoRequest from " + this.getSender();
  }
}
