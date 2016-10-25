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
package org.apache.geode.distributed;

import static com.googlecode.catchexception.CatchException.*;
import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.AbstractLauncher.ServiceState;
import org.apache.geode.distributed.AbstractLauncher.Status;
import org.apache.geode.distributed.LocatorLauncher.LocatorState;
import org.apache.geode.management.internal.cli.json.GfJsonException;
import org.apache.geode.management.internal.cli.json.GfJsonObject;
import org.apache.geode.test.junit.categories.UnitTest;

/**
 * Unit tests for LocatorLauncher.LocatorState
 */
@Category(UnitTest.class)
public class LocatorStateTest {

  private String classpath = "test_classpath";
  private String gemFireVersion = "test_gemfireversion";
  private String host = "test_host";
  private String javaVersion = "test_javaversion";
  private String jvmArguments = "test_jvmarguments";
  private String serviceLocation = "test_location";
  private String logFile = "test_logfile";
  private String memberName = "test_membername";
  private Integer pid = 6396;
  private String port = "test_port";
  private String statusDescription = Status.NOT_RESPONDING.getDescription();
  private String statusMessage = "test_statusmessage";
  private Long timestampTime = 1450728233024L;
  private Long uptime = 1629L;
  private String workingDirectory = "test_workingdirectory";

  @Test
  public void fromJsonWithEmptyStringThrowsIllegalArgumentException() throws Exception {
    // given: empty string
    String emptyString = "";
    
    // when: passed to fromJson
    verifyException(this).fromJson(emptyString);
    
    // then: throws IllegalArgumentException with cause of GfJsonException
    assertThat((Exception)caughtException())
        .isInstanceOf(IllegalArgumentException.class)
        .hasCauseInstanceOf(GfJsonException.class);
    
    assertThat(caughtException().getCause())
        .isInstanceOf(GfJsonException.class)
        .hasNoCause();
  }
  
  @Test
  public void fromJsonWithWhiteSpaceStringThrowsIllegalArgumentException() throws Exception {
    // given: white space string
    String whiteSpaceString = "      ";
    
    // when: passed to fromJson
    verifyException(this).fromJson(whiteSpaceString);

    // then: throws IllegalArgumentException with cause of GfJsonException
    assertThat((Exception)caughtException())
        .isInstanceOf(IllegalArgumentException.class)
        .hasCauseInstanceOf(GfJsonException.class);
    
    assertThat(caughtException().getCause())
        .isInstanceOf(GfJsonException.class)
        .hasNoCause();
  }
  
  @Test
  public void fromJsonWithNullStringThrowsNullPointerException() throws Exception {
    // given: null string
    String nullString = null;
    
    // when: passed to fromJson
    verifyException(this).fromJson(nullString);
    
    // then: throws NullPointerException
    assertThat((Exception)caughtException())
        .isInstanceOf(NullPointerException.class)
        .hasNoCause();
  }
  
  @Test
  public void fromJsonWithValidJsonStringReturnsLocatorState() throws Exception {
    // given: valid json string
    String jsonString = createStatusJson();
    
    // when: passed to fromJson
    LocatorState value = fromJson(jsonString);
    
    // then: return valid instance of LocatorState
    assertThat(value).isInstanceOf(LocatorState.class);
    
    assertThat(value.getClasspath()).isEqualTo(getClasspath());
    assertThat(value.getGemFireVersion()).isEqualTo(getGemFireVersion());
    assertThat(value.getHost()).isEqualTo(getHost());
    assertThat(value.getJavaVersion()).isEqualTo(getJavaVersion());
    assertThat(value.getJvmArguments()).isEqualTo(getJvmArguments());
    assertThat(value.getServiceLocation()).isEqualTo(getServiceLocation());
    assertThat(value.getLogFile()).isEqualTo(getLogFile());
    assertThat(value.getMemberName()).isEqualTo(getMemberName());
    assertThat(value.getPid()).isEqualTo(getPid());
    assertThat(value.getPort()).isEqualTo(getPort());
    assertThat(value.getStatus().getDescription()).isEqualTo(getStatusDescription());
    assertThat(value.getStatusMessage()).isEqualTo(getStatusMessage());
    assertThat(value.getTimestamp().getTime()).isEqualTo(getTimestampTime());
    assertThat(value.getUptime()).isEqualTo(getUptime());
    assertThat(value.getWorkingDirectory()).isEqualTo(getWorkingDirectory());
  }
  
  protected LocatorState fromJson(final String value) {
    return LocatorState.fromJson(value);
  }

  private String getClasspath() {
    return this.classpath;
  }

  private String getGemFireVersion() {
    return this.gemFireVersion;
  }

  private String getHost() {
    return this.host;
  }

  private String getJavaVersion() {
    return this.javaVersion;
  }

  private List<String> getJvmArguments() {
    List<String> list = new ArrayList<String>();
    list.add(this.jvmArguments);
    return list;
  }

  private String getServiceLocation() {
    return this.serviceLocation;
  }

  private String getLogFile() {
    return this.logFile;
  }

  private String getMemberName() {
    return this.memberName;
  }

  private Integer getPid() {
    return this.pid;
  }

  private String getPort() {
    return this.port;
  }

  private String getStatusDescription() {
    return this.statusDescription;
  }

  private String getStatusMessage() {
    return this.statusMessage;
  }

  private Long getTimestampTime() {
    return this.timestampTime;
  }

  private Long getUptime() {
    return this.uptime;
  }

  private String getWorkingDirectory() {
    return this.workingDirectory;
  }

  private String createStatusJson() {
    final Map<String, Object> map = new HashMap<String, Object>();
    map.put(ServiceState.JSON_CLASSPATH, getClasspath());
    map.put(ServiceState.JSON_GEMFIREVERSION, getGemFireVersion());
    map.put(ServiceState.JSON_HOST, getHost());
    map.put(ServiceState.JSON_JAVAVERSION, getJavaVersion());
    map.put(ServiceState.JSON_JVMARGUMENTS, getJvmArguments());
    map.put(ServiceState.JSON_LOCATION, getServiceLocation());
    map.put(ServiceState.JSON_LOGFILE, getLogFile());
    map.put(ServiceState.JSON_MEMBERNAME, getMemberName());
    map.put(ServiceState.JSON_PID, getPid());
    map.put(ServiceState.JSON_PORT, getPort());
    map.put(ServiceState.JSON_STATUS, getStatusDescription());
    map.put(ServiceState.JSON_STATUSMESSAGE, getStatusMessage());
    map.put(ServiceState.JSON_TIMESTAMP, getTimestampTime());
    map.put(ServiceState.JSON_UPTIME, getUptime());
    map.put(ServiceState.JSON_WORKINGDIRECTORY, getWorkingDirectory());
    return new GfJsonObject(map).toString();
  }
}
