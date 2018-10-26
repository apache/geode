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
//
//package org.apache.geode.connectors.jdbc.internal.configuration;
//
//import static org.assertj.core.api.Assertions.assertThat;
//
//import java.net.URL;
//import java.util.List;
//
//import org.junit.Before;
//import org.junit.Test;
//
//import org.apache.geode.cache.configuration.CacheConfig;
//import org.apache.geode.internal.ClassPathLoader;
//import org.apache.geode.internal.config.JAXBService;
//
//
//public class ConnectorServiceTest {
//
//  private JAXBService jaxbService;
//
//  @Before
//  public void setUp() throws Exception {
//    jaxbService = new JAXBService(CacheConfig.class, ConnectorService.class);
//    // find the local jdbc-1.0.xsd
//    URL local_xsd = ClassPathLoader.getLatest()
//        .getResource("META-INF/schemas/geode.apache.org/schema/jdbc/jdbc-1.0.xsd");
//    jaxbService.validateWith(local_xsd);
//  }
//
//  @Test
//  public void regionMappingTest() {
//    ConnectorService service = new ConnectorService();
//    RegionMapping mapping = new RegionMapping();
//    mapping.setConnectionConfigName("configName");
//    mapping.setPdxClassName("pdxClassName");
//    mapping.setRegionName("regionA");
//    mapping.setTableName("tableName");
//    mapping.getFieldMapping()
//        .add(new RegionMapping.FieldMapping("field1", "column1"));
//    mapping.getFieldMapping()
//        .add(new RegionMapping.FieldMapping("field2", "column2"));
//
//    service.getRegionMapping().add(mapping);
//    String xml = jaxbService.marshall(service);
//
//    assertThat(xml).contains("jdbc:connector-service").contains("connection-name=\"configName\" ")
//        .contains("pdx-class=\"pdxClassName\"")
//        .contains("<jdbc:field-mapping field-name=\"field1\" column-name=\"column1\"/>");
//    System.out.println(xml);
//
//    ConnectorService service2 = jaxbService.unMarshall(xml);
//    assertThat(service2.getRegionMapping()).hasSize(1);
//    List<RegionMapping.FieldMapping> mappings =
//        service2.getRegionMapping().get(0).getFieldMapping();
//
//    assertThat(mappings.get(0).getFieldName()).isEqualTo("field1");
//    assertThat(mappings.get(0).getColumnName()).isEqualTo("column1");
//    assertThat(mappings.get(1).getFieldName()).isEqualTo("field2");
//    assertThat(mappings.get(1).getColumnName()).isEqualTo("column2");
//  }
//}
