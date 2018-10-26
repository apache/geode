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
// package org.apache.geode.connectors.jdbc.internal.xml;
//
//
//
//
// public class JdbcServiceConfigurationTest {
//
// private InternalCache cache;
// private JdbcConnectorService service;
// private RegionMapping mapping1;
// private RegionMapping mapping2;
//
// private JdbcServiceConfiguration configuration;
//
// @Before
// public void setUp() throws Exception {
// mapping1 = mock(RegionMapping.class);
// mapping2 = mock(RegionMapping.class);
// service = mock(JdbcConnectorService.class);
// cache = mock(InternalCache.class);
//
// when(cache.getService(JdbcConnectorService.class)).thenReturn(service);
//
// configuration = new JdbcServiceConfiguration();
// }
//
// @Test
// public void onCreateWithNoConnectionsOrMappings() throws Exception {
// configuration.onCreate(cache, cache);
//
// verifyZeroInteractions(service);
// }
//
// @Test
// public void onCreateWithRegionMappings() throws Exception {
// configuration.addRegionMapping(mapping1);
// configuration.addRegionMapping(mapping2);
//
// configuration.onCreate(cache, cache);
//
// verify(service, times(1)).createRegionMapping(mapping1);
// verify(service, times(1)).createRegionMapping(mapping2);
// }
//
// }
