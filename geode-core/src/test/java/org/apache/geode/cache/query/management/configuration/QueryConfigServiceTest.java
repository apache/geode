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

package org.apache.geode.cache.query.management.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.internal.config.JAXBService;

public class QueryConfigServiceTest {

  private JAXBService service;
  private QueryConfigService config;

  @Before
  public void before() throws Exception {
    service = new JAXBService(QueryConfigService.class);
    config = new QueryConfigService();
  }

  @Test
  public void marshallAndUnmarshallDoNotThrowExceptions() {
    QueryConfigService.MethodAuthorizer authorizer =
        new QueryConfigService.MethodAuthorizer();
    String className = "className";
    authorizer.setClassName(className);

    List<QueryConfigService.MethodAuthorizer.Parameter> paramList = new ArrayList<>();
    QueryConfigService.MethodAuthorizer.Parameter parameter1 =
        new QueryConfigService.MethodAuthorizer.Parameter();
    String param1 = "param1";
    parameter1.setParameterValue(param1);
    QueryConfigService.MethodAuthorizer.Parameter parameter2 =
        new QueryConfigService.MethodAuthorizer.Parameter();
    String param2 = "param2";
    parameter2.setParameterValue(param2);
    paramList.add(parameter1);
    paramList.add(parameter2);
    authorizer.setParameters(paramList);

    config.setMethodAuthorizer(authorizer);
    String marshalledXml = service.marshall(config);
    QueryConfigService service1 =
        service.unMarshall(marshalledXml, QueryConfigService.class);

    assertThat(service1.getMethodAuthorizer().getClassName()).isEqualTo(className);
    assertThat(service1.getMethodAuthorizer().getParameters().size()).isEqualTo(paramList.size());
    IntStream.range(0, paramList.size()).forEach(
        i -> assertThat(service1.getMethodAuthorizer().getParameters().get(i).getParameterValue())
            .isEqualTo(paramList.get(i).getParameterValue()));
  }
}
