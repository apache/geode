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

package org.apache.geode.cache.client.internal.locator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.lang.ObjectUtils;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.internal.process.PidUnavailableException;
import org.apache.geode.internal.process.ProcessUtils;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.util.ArgumentRedactor;

/**
 * The LocatorStatusResponse class...
 * </p>
 *
 * @see org.apache.geode.cache.client.internal.locator.ServerLocationResponse
 * @since GemFire 7.0
 */
public class LocatorStatusResponse extends ServerLocationResponse {

  private Integer pid;

  private List<String> jvmArgs;

  private Long uptime;

  private String classpath;
  private String gemfireVersion;
  private String javaVersion;
  private String workingDirectory;

  private String logFile;
  private String host;
  private Integer port;
  private String name;

  private static Integer identifyPid() {
    try {
      return ProcessUtils.identifyPid();
    } catch (PidUnavailableException ignore) {
      return null;
    }
  }

  public LocatorStatusResponse initialize(final int locatorPort, final String locatorHost,
      final String locatorLogFile, final String locatorName) {
    final RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
    pid = identifyPid();
    jvmArgs = ArgumentRedactor.redactEachInList(runtimeBean.getInputArguments());
    uptime = runtimeBean.getUptime();
    classpath = runtimeBean.getClassPath();
    gemfireVersion = GemFireVersion.getGemFireVersion();
    javaVersion = System.getProperty("java.version");
    workingDirectory = System.getProperty("user.dir");
    logFile = locatorLogFile;
    host = locatorHost;
    port = locatorPort;
    name = locatorName;
    return this;
  }

  @Override
  public int getDSFID() {
    return DataSerializableFixedID.LOCATOR_STATUS_RESPONSE;
  }

  public String getClasspath() {
    return classpath;
  }

  public String getGemFireVersion() {
    return gemfireVersion;
  }

  public String getJavaVersion() {
    return javaVersion;
  }

  public List<String> getJvmArgs() {
    return Collections.unmodifiableList(jvmArgs != null ? jvmArgs : Collections.emptyList());
  }

  public Integer getPid() {
    return pid;
  }

  public Long getUptime() {
    return uptime;
  }

  public String getWorkingDirectory() {
    return workingDirectory;
  }

  public String getLogFile() {
    return logFile;
  }

  public String getHost() {
    return host;
  }

  public Integer getPort() {
    return port;
  }

  public String getName() {
    return name;
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public void fromData(final DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    readPid(in);
    readUptime(in);
    readWorkingDirectory(in);
    readJvmArguments(in);
    readClasspath(in);
    readGemFireVersion(in);
    readJavaVersion(in);
    readLogFile(in);
    readHost(in);
    readPort(in);
    readName(in);
  }

  protected void readPid(final DataInput in) throws IOException {
    final int pid = in.readInt();
    this.pid = (pid == 0 ? null : pid);
  }

  protected void readUptime(final DataInput in) throws IOException {
    uptime = in.readLong();
  }

  protected void readWorkingDirectory(final DataInput in) throws IOException {
    workingDirectory = StringUtils.nullifyIfBlank(in.readUTF());
  }

  protected void readJvmArguments(final DataInput in) throws IOException {
    final int length = in.readInt();
    final List<String> jvmArgs = new ArrayList<>(length);
    for (int index = 0; index < length; index++) {
      jvmArgs.add(in.readUTF());
    }
    this.jvmArgs = jvmArgs;
  }

  protected void readClasspath(final DataInput in) throws IOException {
    classpath = StringUtils.nullifyIfBlank(in.readUTF());
  }

  protected void readGemFireVersion(final DataInput in) throws IOException {
    gemfireVersion = StringUtils.nullifyIfBlank(in.readUTF());
  }

  protected void readJavaVersion(final DataInput in) throws IOException {
    javaVersion = StringUtils.nullifyIfBlank(in.readUTF());
  }

  protected void readLogFile(final DataInput in) throws IOException {
    logFile = StringUtils.nullifyIfBlank(in.readUTF());
  }

  protected void readHost(final DataInput in) throws IOException {
    host = StringUtils.nullifyIfBlank(in.readUTF());
  }

  protected void readPort(final DataInput in) throws IOException {
    final int port = in.readInt();
    this.port = (port == 0 ? null : port);
  }

  protected void readName(final DataInput in) throws IOException {
    name = StringUtils.nullifyIfBlank(in.readUTF());
  }

  @Override
  public void toData(final DataOutput out,
      SerializationContext context) throws IOException {
    writePid(out);
    writeUptime(out);
    writeWorkingDirectory(out);
    writeJvmArguments(out);
    writeClasspath(out);
    writeGemFireVersion(out);
    writeJavaVersion(out);
    writeLogFile(out);
    writeHost(out);
    writePort(out);
    writeName(out);
  }

  protected void writePid(final DataOutput out) throws IOException {
    Integer pid = getPid();
    out.writeInt(pid != null ? pid : (Integer) 0);
  }

  protected void writeUptime(final DataOutput out) throws IOException {
    out.writeLong(getUptime());
  }

  protected void writeWorkingDirectory(final DataOutput out) throws IOException {
    String workingDir = getWorkingDirectory();
    out.writeUTF(workingDir != null ? workingDir : "");
  }

  protected void writeJvmArguments(final DataOutput out) throws IOException {
    final List<String> jvmArgs = getJvmArgs();
    out.writeInt(jvmArgs.size());
    for (final String jvmArg : jvmArgs) {
      out.writeUTF(jvmArg);
    }
  }

  protected void writeClasspath(final DataOutput out) throws IOException {
    String classpath = getClasspath();
    out.writeUTF(classpath != null ? classpath : "");
  }

  protected void writeGemFireVersion(final DataOutput out) throws IOException {
    String version = getGemFireVersion();
    out.writeUTF(version != null ? version : "");
  }

  protected void writeJavaVersion(final DataOutput out) throws IOException {
    String version = getJavaVersion();
    out.writeUTF(version != null ? version : "");
  }

  protected void writeLogFile(final DataOutput out) throws IOException {
    String log = getLogFile();
    out.writeUTF(log != null ? log : "");
  }

  protected void writeHost(final DataOutput out) throws IOException {
    String host = getHost();
    out.writeUTF(host != null ? host : "");
  }

  protected void writePort(final DataOutput out) throws IOException {
    Integer port = getPort();
    out.writeInt(port != null ? port : (Integer) 0);
  }

  protected void writeName(final DataOutput out) throws IOException {
    String name = getName();
    out.writeUTF(name != null ? name : "");
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }

    if (!(obj instanceof LocatorStatusResponse)) {
      return false;
    }

    final LocatorStatusResponse that = (LocatorStatusResponse) obj;

    return ObjectUtils.equalsIgnoreNull(getPid(), that.getPid())
        && ObjectUtils.equals(getUptime(), that.getUptime())
        && ObjectUtils.equals(getWorkingDirectory(), that.getWorkingDirectory())
        && ObjectUtils.equals(getJvmArgs(), that.getJvmArgs())
        && ObjectUtils.equals(getClasspath(), that.getClasspath())
        && ObjectUtils.equals(getGemFireVersion(), that.getGemFireVersion())
        && ObjectUtils.equals(getJavaVersion(), that.getJavaVersion());
  }

  @Override
  public int hashCode() {
    int hashValue = 17;
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getPid());
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getUptime());
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getWorkingDirectory());
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getJvmArgs());
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getClasspath());
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getGemFireVersion());
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getJavaVersion());
    return hashValue;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{ pid = " + getPid()
        + ", uptime = " + getUptime()
        + ", workingDirectory = " + getWorkingDirectory()
        + ", jvmArgs = " + ArgumentRedactor.redact(getJvmArgs())
        + ", classpath = " + getClasspath()
        + ", gemfireVersion = " + getGemFireVersion()
        + ", javaVersion = " + getJavaVersion()
        + "}";
  }

}
