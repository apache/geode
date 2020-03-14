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
package org.apache.geode.experimental.driver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.protobuf.ProtocolStringList;

import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes.EncodedValue;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes.Table;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol.Message;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol.Message.MessageTypeCase;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI.OQLQueryRequest;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI.OQLQueryResponse;

class ProtobufQueryService implements QueryService {
  private final ProtobufChannel channel;
  private final ValueEncoder valueEncoder;

  public ProtobufQueryService(ProtobufChannel channel, ValueEncoder valueEncoder) {
    this.channel = channel;
    this.valueEncoder = valueEncoder;
  }

  @Override
  public <T> Query<T> newQuery(final String queryString) {
    return new ProtobufQuery<>(queryString);
  }

  class ProtobufQuery<T> implements Query<T> {

    private final String queryString;

    public ProtobufQuery(final String queryString) {
      this.queryString = queryString;
    }

    @Override
    public List<T> execute(final Object... bindParameters) throws IOException {
      List<EncodedValue> encodedParameters = Arrays.stream(bindParameters)
          .map(valueEncoder::encodeValue).collect(Collectors.toList());
      Message request = Message.newBuilder().setOqlQueryRequest(
          OQLQueryRequest.newBuilder().addAllBindParameter(encodedParameters).setQuery(queryString))
          .build();
      final OQLQueryResponse response =
          channel.sendRequest(request, MessageTypeCase.OQLQUERYRESPONSE).getOqlQueryResponse();
      switch (response.getResultCase()) {
        case SINGLERESULT:
          return parseSingleResult(response);
        case LISTRESULT:
          return parseListResult(response);
        case TABLERESULT:
          @SuppressWarnings("unchecked")
          final List<T> tableResult = (List<T>) parseTableResult(response);
          return tableResult;
        default:
          throw new RuntimeException("Unexpected response: " + response);
      }
    }

    private List<Map<String, Object>> parseTableResult(final OQLQueryResponse response) {
      final Table table = response.getTableResult();
      final ProtocolStringList fieldNames = table.getFieldNameList();
      List<Map<String, Object>> results = new ArrayList<>();
      for (BasicTypes.EncodedValueList row : table.getRowList()) {
        final List<Object> decodedRow = row.getElementList().stream().map(valueEncoder::decodeValue)
            .collect(Collectors.toList());

        Map<String, Object> rowMap = new LinkedHashMap<>(decodedRow.size());
        for (int i = 0; i < decodedRow.size(); i++) {
          rowMap.put(fieldNames.get(i), decodedRow.get(i));
        }
        results.add(rowMap);
      }

      return results;
    }

    private List<T> parseListResult(final OQLQueryResponse response) {
      return response.getListResult().getElementList().stream()
          .map(valueEncoder::<T>decodeValue).collect(Collectors.toList());
    }

    private List<T> parseSingleResult(final OQLQueryResponse response) {
      return Collections.singletonList(valueEncoder.decodeValue(response.getSingleResult()));
    }
  }

}
