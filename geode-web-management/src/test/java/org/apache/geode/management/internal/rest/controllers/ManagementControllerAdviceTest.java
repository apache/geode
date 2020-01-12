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

package org.apache.geode.management.internal.rest.controllers;

import static org.apache.geode.management.internal.rest.controllers.ManagementControllerAdvice.removeClassFromJsonText;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class ManagementControllerAdviceTest {

  @Test
  public void removesClassAttributeWhenThereAreOtherAttributes() {
    String before =
        "{\"statusCode\":\"OK\",\"statusMessage\":null,\"uri\":null,\"result\":[{\"configuration\":{\"class\":\"org.apache.geode.management.configuration.Member\",\"group\":null,\"id\":\"locator-0\",\"uri\":\"/management/v1/members/locator-0\"},\"runtimeInfo\":[{\"class\":\"org.apache.geode.management.runtime.MemberInformation\",\"memberName\":\"locator-0\",\"id\":\"10.0.0.2(locator-0:81136:locator)<ec><v0>:41000\",\"workingDirPath\":\"/geode/geode-web-management\",\"groups\":\"\",\"logFilePath\":\"/private/var/folders/y_/d_csfs5966gdq5rgt_n6wy040000gn/T/junit8812600995087886644/locator-0.log\",\"statArchiveFilePath\":\"/geode/geode-web-management\",\"serverBindAddress\":\"\",\"locators\":\"10.0.0.2[51393]\",\"status\":\"online\",\"heapUsage\":304,\"maxHeapSize\":3641,\"initHeapSize\":256,\"cacheXmlFilePath\":\"/geode/geode-web-management\",\"host\":\"10.0.0.2\",\"processId\":81136,\"locatorPort\":51393,\"httpServicePort\":7070,\"httpServiceBindAddress\":\"\",\"clientCount\":0,\"cpuUsage\":0.0,\"hostedRegions\":[],\"offHeapMemorySize\":\"\",\"webSSL\":false,\"server\":false,\"cacheServerInfo\":[],\"coordinator\":true,\"secured\":false}]}]}";
    String after =
        "{\"statusCode\":\"OK\",\"statusMessage\":null,\"uri\":null,\"result\":[{\"configuration\":{\"group\":null,\"id\":\"locator-0\",\"uri\":\"/management/v1/members/locator-0\"},\"runtimeInfo\":[{\"memberName\":\"locator-0\",\"id\":\"10.0.0.2(locator-0:81136:locator)<ec><v0>:41000\",\"workingDirPath\":\"/geode/geode-web-management\",\"groups\":\"\",\"logFilePath\":\"/private/var/folders/y_/d_csfs5966gdq5rgt_n6wy040000gn/T/junit8812600995087886644/locator-0.log\",\"statArchiveFilePath\":\"/geode/geode-web-management\",\"serverBindAddress\":\"\",\"locators\":\"10.0.0.2[51393]\",\"status\":\"online\",\"heapUsage\":304,\"maxHeapSize\":3641,\"initHeapSize\":256,\"cacheXmlFilePath\":\"/geode/geode-web-management\",\"host\":\"10.0.0.2\",\"processId\":81136,\"locatorPort\":51393,\"httpServicePort\":7070,\"httpServiceBindAddress\":\"\",\"clientCount\":0,\"cpuUsage\":0.0,\"hostedRegions\":[],\"offHeapMemorySize\":\"\",\"webSSL\":false,\"server\":false,\"cacheServerInfo\":[],\"coordinator\":true,\"secured\":false}]}]}";
    assertThat(removeClassFromJsonText(before)).isEqualTo(after);
  }

  @Test
  public void removesClassAttributeAndContainingObjectWhenThereAreNoOtherAttributes() {
    String before =
        "{\"statusCode\":\"OK\",\"result\":[{\"configuration\":{\"class\":\"org.apache.geode.management.configuration.Member\"},\"runtimeInfo\":[{\"class\":\"org.apache.geode.management.runtime.MemberInformation\",\"memberName\":\"locator-0\",\"id\":\"10.0.0.2(locator-0:81569:locator)<ec><v0>:41000\",\"workingDirPath\":\"/geode/geode-web-management\",\"logFilePath\":\"/private/var/folders/y_/d_csfs5966gdq5rgt_n6wy040000gn/T/junit2892068854793149212/locator-0.log\",\"statArchiveFilePath\":\"/geode/geode-web-management\",\"locators\":\"10.0.0.2[51844]\",\"status\":\"online\",\"heapUsage\":171,\"maxHeapSize\":3641,\"initHeapSize\":256,\"cacheXmlFilePath\":\"/geode/geode-web-management\",\"host\":\"10.0.0.2\",\"processId\":81569,\"locatorPort\":51844,\"httpServicePort\":7070,\"clientCount\":0,\"cpuUsage\":0.0,\"webSSL\":false,\"coordinator\":true,\"server\":false,\"secured\":false},{\"class\":\"org.apache.geode.management.runtime.MemberInformation\",\"memberName\":\"server-1\",\"id\":\"10.0.0.2(server-1:81573)<v1>:41002\",\"workingDirPath\":\"/geode/geode-web-management/dunit/vm1\",\"groups\":\"group-1,group-2\",\"logFilePath\":\"/geode/geode-web-management/dunit/vm1/server-1.log\",\"statArchiveFilePath\":\"/geode/geode-web-management/dunit/vm1\",\"locators\":\"localhost[51844]\",\"heapUsage\":29,\"maxHeapSize\":455,\"initHeapSize\":256,\"cacheXmlFilePath\":\"/geode/geode-web-management/dunit/vm1/cache.xml\",\"host\":\"10.0.0.2\",\"processId\":81573,\"locatorPort\":0,\"httpServicePort\":0,\"clientCount\":0,\"cpuUsage\":0.0,\"webSSL\":false,\"coordinator\":false,\"server\":true,\"cacheServerInfo\":[{\"port\":51888,\"maxConnections\":800,\"maxThreads\":0,\"running\":true}],\"secured\":false}]}]}";
    String after =
        "{\"statusCode\":\"OK\",\"result\":[{\"runtimeInfo\":[{\"memberName\":\"locator-0\",\"id\":\"10.0.0.2(locator-0:81569:locator)<ec><v0>:41000\",\"workingDirPath\":\"/geode/geode-web-management\",\"logFilePath\":\"/private/var/folders/y_/d_csfs5966gdq5rgt_n6wy040000gn/T/junit2892068854793149212/locator-0.log\",\"statArchiveFilePath\":\"/geode/geode-web-management\",\"locators\":\"10.0.0.2[51844]\",\"status\":\"online\",\"heapUsage\":171,\"maxHeapSize\":3641,\"initHeapSize\":256,\"cacheXmlFilePath\":\"/geode/geode-web-management\",\"host\":\"10.0.0.2\",\"processId\":81569,\"locatorPort\":51844,\"httpServicePort\":7070,\"clientCount\":0,\"cpuUsage\":0.0,\"webSSL\":false,\"coordinator\":true,\"server\":false,\"secured\":false},{\"memberName\":\"server-1\",\"id\":\"10.0.0.2(server-1:81573)<v1>:41002\",\"workingDirPath\":\"/geode/geode-web-management/dunit/vm1\",\"groups\":\"group-1,group-2\",\"logFilePath\":\"/geode/geode-web-management/dunit/vm1/server-1.log\",\"statArchiveFilePath\":\"/geode/geode-web-management/dunit/vm1\",\"locators\":\"localhost[51844]\",\"heapUsage\":29,\"maxHeapSize\":455,\"initHeapSize\":256,\"cacheXmlFilePath\":\"/geode/geode-web-management/dunit/vm1/cache.xml\",\"host\":\"10.0.0.2\",\"processId\":81573,\"locatorPort\":0,\"httpServicePort\":0,\"clientCount\":0,\"cpuUsage\":0.0,\"webSSL\":false,\"coordinator\":false,\"server\":true,\"cacheServerInfo\":[{\"port\":51888,\"maxConnections\":800,\"maxThreads\":0,\"running\":true}],\"secured\":false}]}]}";
    assertThat(removeClassFromJsonText(before)).isEqualTo(after);
  }
}
