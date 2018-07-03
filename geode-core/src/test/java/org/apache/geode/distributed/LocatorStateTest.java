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
package org.apache.geode.distributed;

import static com.googlecode.catchexception.CatchException.caughtException;
import static com.googlecode.catchexception.CatchException.verifyException;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.AbstractLauncher.ServiceState;
import org.apache.geode.distributed.AbstractLauncher.Status;
import org.apache.geode.distributed.LocatorLauncher.LocatorState;
import org.apache.geode.management.internal.cli.json.GfJsonException;
import org.apache.geode.management.internal.cli.json.GfJsonObject;

/**
 * Unit tests for {@link LocatorLauncher.LocatorState}.
 */
public class LocatorStateTest {

  private String classpath;
  private String gemFireVersion;
  private String host;
  private String javaVersion;
  private String jvmArguments;
  private String serviceLocation;
  private String logFile;
  private String memberName;
  private Integer pid;
  private String port;
  private String statusDescription;
  private String statusMessage;
  private Long timestampTime;
  private Long uptime;
  private String workingDirectory;

  @Before
  public void before() throws Exception {
    classpath = "test_classpath";
    gemFireVersion = "test_gemfireVersion";
    host = "test_host";
    javaVersion = "test_javaVersion";
    jvmArguments = "test_jvmArguments";
    serviceLocation = "test_location";
    logFile = "test_logfile";
    memberName = "test_memberName";
    pid = 6396;
    port = "test_port";
    statusDescription = Status.NOT_RESPONDING.getDescription();
    statusMessage = "test_statusMessage";
    timestampTime = 1450728233024L;
    uptime = 1629L;
    workingDirectory = "test_workingDirectory";
  }

  @Test
  public void fromJsonWithEmptyStringThrowsIllegalArgumentException() throws Exception {
    // given: empty string
    String emptyString = "";

    // when: passed to fromJson
    verifyException(this).fromJson(emptyString);

    // then: throws IllegalArgumentException with cause of GfJsonException
    assertThat((Exception) caughtException()).isInstanceOf(IllegalArgumentException.class)
        .hasCauseInstanceOf(GfJsonException.class);
    assertThat(caughtException().getCause()).isInstanceOf(GfJsonException.class).hasNoCause();
  }

  @Test
  public void fromJsonWithWhiteSpaceStringThrowsIllegalArgumentException() throws Exception {
    // given: white space string
    String whiteSpaceString = "      ";

    // when: passed to fromJson
    verifyException(this).fromJson(whiteSpaceString);

    // then: throws IllegalArgumentException with cause of GfJsonException
    assertThat((Exception) caughtException()).isInstanceOf(IllegalArgumentException.class)
        .hasCauseInstanceOf(GfJsonException.class);
    assertThat(caughtException().getCause()).isInstanceOf(GfJsonException.class).hasNoCause();
  }

  @Test
  public void fromJsonWithNullStringThrowsNullPointerException() throws Exception {
    // given: null string
    String nullString = null;

    // when: passed to fromJson
    verifyException(this).fromJson(nullString);

    // then: throws NullPointerException
    assertThat((Exception) caughtException()).isInstanceOf(NullPointerException.class).hasNoCause();
  }

  @Test
  public void fromJsonWithValidJsonStringReturnsLocatorState() throws Exception {
    // given: valid json string
    String jsonString = createStatusJson();

    // when: passed to fromJson
    LocatorState value = fromJson(jsonString);

    // then: return valid instance of LocatorState
    assertThat(value).isInstanceOf(LocatorState.class);
    assertThat(value.getClasspath()).isEqualTo(classpath);
    assertThat(value.getGemFireVersion()).isEqualTo(gemFireVersion);
    assertThat(value.getHost()).isEqualTo(host);
    assertThat(value.getJavaVersion()).isEqualTo(javaVersion);
    assertThat(value.getJvmArguments()).isEqualTo(getJvmArguments());
    assertThat(value.getLogFile()).isEqualTo(logFile);
    assertThat(value.getMemberName()).isEqualTo(memberName);
    assertThat(value.getPid()).isEqualTo(pid);
    assertThat(value.getPort()).isEqualTo(port);
    assertThat(value.getServiceLocation()).isEqualTo(serviceLocation);
    assertThat(value.getStatus().getDescription()).isEqualTo(statusDescription);
    assertThat(value.getStatusMessage()).isEqualTo(statusMessage);
    assertThat(value.getTimestamp().getTime()).isEqualTo(timestampTime);
    assertThat(value.getUptime()).isEqualTo(uptime);
    assertThat(value.getWorkingDirectory()).isEqualTo(workingDirectory);
  }

  /**
   * NOTE: Must be protected for CatchException.
   */
  protected LocatorState fromJson(final String value) {
    return LocatorState.fromJson(value);
  }

  private List<String> getJvmArguments() {
    List<String> list = new ArrayList<>();
    list.add(jvmArguments);
    return list;
  }

  private String createStatusJson() {
    Map<String, Object> map = new HashMap<>();
    map.put(ServiceState.JSON_CLASSPATH, classpath);
    map.put(ServiceState.JSON_GEMFIREVERSION, gemFireVersion);
    map.put(ServiceState.JSON_HOST, host);
    map.put(ServiceState.JSON_JAVAVERSION, javaVersion);
    map.put(ServiceState.JSON_JVMARGUMENTS, getJvmArguments());
    map.put(ServiceState.JSON_LOCATION, serviceLocation);
    map.put(ServiceState.JSON_LOGFILE, logFile);
    map.put(ServiceState.JSON_MEMBERNAME, memberName);
    map.put(ServiceState.JSON_PID, pid);
    map.put(ServiceState.JSON_PORT, port);
    map.put(ServiceState.JSON_STATUS, statusDescription);
    map.put(ServiceState.JSON_STATUSMESSAGE, statusMessage);
    map.put(ServiceState.JSON_TIMESTAMP, timestampTime);
    map.put(ServiceState.JSON_UPTIME, uptime);
    map.put(ServiceState.JSON_WORKINGDIRECTORY, workingDirectory);
    return new GfJsonObject(map).toString();
  }
}
