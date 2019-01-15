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
package org.apache.geode.connectors.util.internal;

import static org.apache.geode.connectors.util.internal.MappingConstants.DATA_SOURCE_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.ID_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.PDX_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.REGION_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.SYNCHRONOUS_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.TABLE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.junit.Test;

public class DescribeMappingResultTest {

  @Test
  public void testSeriazilabeToAndFromByteArray() throws IOException {
    Map<String, String> attributesMap = new HashMap<>();
    attributesMap.put(REGION_NAME, "myRegion");
    attributesMap.put(TABLE_NAME, "myTable");
    attributesMap.put(PDX_NAME, "myPdx");
    attributesMap.put(DATA_SOURCE_NAME, "myDatasource");
    attributesMap.put(SYNCHRONOUS_NAME, "false");
    attributesMap.put(ID_NAME, "myId");

    DescribeMappingResult result = new DescribeMappingResult(attributesMap);
    DescribeMappingResult newResult = new DescribeMappingResult();

    ByteArrayDataOutputStream output = new ByteArrayDataOutputStream();
    ByteArrayDataInputStream input;

    result.toData(output);

    input = new ByteArrayDataInputStream(output.buffer());

    newResult.fromData(input);

    Map<String, String> newAttributeMap = newResult.getAttributeMap();
    assertThat(attributesMap).isEqualTo(newAttributeMap);
  }

}
