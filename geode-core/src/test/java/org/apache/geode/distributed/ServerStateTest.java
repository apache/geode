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

import static org.apache.geode.distributed.AbstractLauncher.ServiceState.TO_STRING_STATUS_MESSAGE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ServerLauncher.ServerState}.
 */
public class ServerStateTest {

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

  @BeforeEach
  public void setUp() {
    classpath = "test_classpath";
    gemFireVersion = "test_gemfireVersion";
    host = "test_host";
    javaVersion = "test_javaVersion";
    jvmArguments = "test_jvmArguments";
    serviceLocation = "test_location";
    logFile = "test_logfile";
    memberName = "test_memberName";
    pid = 6398;
    port = "test_port";
    statusDescription = AbstractLauncher.Status.NOT_RESPONDING.getDescription();
    statusMessage = "test_statusMessage";
    timestampTime = 1450728233024L;
    uptime = 16291L;
    workingDirectory = "test_workingDirectory";
  }


  @Test
  public void fromJsonWithEmptyStringThrowsIllegalArgumentException() {
    // given: empty string
    String emptyString = "";

    // when: passed to fromJson
    Throwable thrown = catchThrowable(() -> fromJson(emptyString));

    assertThat(thrown).isInstanceOf(IllegalArgumentException.class)
        .hasCauseInstanceOf(NullPointerException.class);
    assertThat(thrown.getCause()).isInstanceOf(NullPointerException.class).hasNoCause();
  }

  @Test
  public void fromJsonWithWhiteSpaceStringThrowsIllegalArgumentException() {
    // given: white space string
    String whiteSpaceString = "      ";

    // when: passed to fromJson
    Throwable thrown = catchThrowable(() -> fromJson(whiteSpaceString));

    // then: throws IllegalArgumentException with cause of NullPointerException
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class)
        .hasCauseInstanceOf(NullPointerException.class);
    assertThat(thrown.getCause()).isInstanceOf(NullPointerException.class).hasNoCause();
  }

  @Test
  public void fromJsonWithNullStringThrowsNullPointerException() {
    // given: null string
    String nullString = null;

    // when: passed to fromJson
    Throwable thrown = catchThrowable(() -> fromJson(nullString));

    // then: throws IllegalArgumentException
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class)
        .hasCauseInstanceOf(IllegalArgumentException.class);
    assertThat(thrown.getCause()).isInstanceOf(IllegalArgumentException.class).hasNoCause();

  }


  @Test
  public void fromJsonWithValidJsonStringReturnsServerState() throws JsonProcessingException {
    // given: valid json string
    String jsonString = createStatusJson();

    // when: passed to fromJson
    ServerLauncher.ServerState value = fromJson(jsonString);

    // then: return valid instance of ServerState
    assertThat(value).isInstanceOf(ServerLauncher.ServerState.class);
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


  @Test
  public void fromJsonWithValidJsonStringReturnsServerStateToStringNoStatusMessage()
      throws JsonProcessingException {
    // given: valid json string
    String jsonString = createStatusJson();

    // when: passed to fromJson
    ServerLauncher.ServerState value = fromJson(jsonString);

    String toString = value.toString();
    assertThat(toString.contains(TO_STRING_STATUS_MESSAGE)).isFalse();

  }

  @Test
  public void fromJsonWithValidJsonStringReturnsServerStateToStringHasStatusMessage()
      throws JsonProcessingException {
    // given: valid json string
    statusDescription = AbstractLauncher.Status.STARTING.getDescription();
    String jsonString = createStatusJson();

    // when: passed to fromJson
    ServerLauncher.ServerState value = fromJson(jsonString);

    String toString = value.toString();
    assertThat(toString.contains(TO_STRING_STATUS_MESSAGE)).isTrue();

  }

  private ServerLauncher.ServerState fromJson(final String value) {
    return ServerLauncher.ServerState.fromJson(value);
  }

  private List<String> getJvmArguments() {
    List<String> list = new ArrayList<>();
    list.add(jvmArguments);
    return list;
  }

  private String createStatusJson() throws JsonProcessingException {
    Map<String, Object> map = new HashMap<>();
    map.put(AbstractLauncher.ServiceState.JSON_CLASSPATH, classpath);
    map.put(AbstractLauncher.ServiceState.JSON_GEMFIREVERSION, gemFireVersion);
    map.put(AbstractLauncher.ServiceState.JSON_HOST, host);
    map.put(AbstractLauncher.ServiceState.JSON_JAVAVERSION, javaVersion);
    map.put(AbstractLauncher.ServiceState.JSON_JVMARGUMENTS, getJvmArguments());
    map.put(AbstractLauncher.ServiceState.JSON_LOCATION, serviceLocation);
    map.put(AbstractLauncher.ServiceState.JSON_LOGFILE, logFile);
    map.put(AbstractLauncher.ServiceState.JSON_MEMBERNAME, memberName);
    map.put(AbstractLauncher.ServiceState.JSON_PID, pid);
    map.put(AbstractLauncher.ServiceState.JSON_PORT, port);
    map.put(AbstractLauncher.ServiceState.JSON_STATUS, statusDescription);
    map.put(AbstractLauncher.ServiceState.JSON_STATUSMESSAGE, statusMessage);
    map.put(AbstractLauncher.ServiceState.JSON_TIMESTAMP, timestampTime);
    map.put(AbstractLauncher.ServiceState.JSON_UPTIME, uptime);
    map.put(AbstractLauncher.ServiceState.JSON_WORKINGDIRECTORY, workingDirectory);

    String status = new ObjectMapper().writeValueAsString(map);

    return status;
  }
}
