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

package org.apache.geode.cache.client.internal.locator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.lang.ObjectUtils;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.internal.process.PidUnavailableException;
import org.apache.geode.internal.process.ProcessUtils;

/**
 * The LocatorStatusResponse class...
 * </p>
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
    }
    catch (PidUnavailableException ignore) {
      return null;
    }
  }

  public LocatorStatusResponse initialize(final int locatorPort,
                                          final String locatorHost,
                                          final String locatorLogFile,
                                          final String locatorName) {
    final RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
    this.pid = identifyPid();
    this.jvmArgs = runtimeBean.getInputArguments();
    this.uptime = runtimeBean.getUptime();
    this.classpath = runtimeBean.getClassPath();
    this.gemfireVersion = GemFireVersion.getGemFireVersion();
    this.javaVersion = System.getProperty("java.version");
    this.workingDirectory = System.getProperty("user.dir");
    this.logFile = locatorLogFile;
    this.host = locatorHost;
    this.port = locatorPort;
    this.name = locatorName;
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

  @SuppressWarnings("unchecked")
  public List<String> getJvmArgs() {
    return Collections.unmodifiableList(ObjectUtils.defaultIfNull(jvmArgs, Collections.<String>emptyList()));
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
    return this.logFile;
  }
  
  public String getHost() {
    return this.host;
  }
  
  public Integer getPort() {
    return this.port;
  }
  
  public String getName() {
    return this.name;
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public void fromData(final DataInput in) throws IOException, ClassNotFoundException {
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
    this.uptime = in.readLong();
  }

  protected void readWorkingDirectory(final DataInput in) throws IOException {
    this.workingDirectory = StringUtils.defaultIfBlank(in.readUTF());
  }

  protected void readJvmArguments(final DataInput in) throws IOException {
    final int length = in.readInt();
    final List<String> jvmArgs = new ArrayList<String>(length);
    for (int index = 0; index < length; index++) {
      jvmArgs.add(in.readUTF());
    }
    this.jvmArgs = jvmArgs;
  }

  protected void readClasspath(final DataInput in) throws IOException {
    this.classpath = StringUtils.defaultIfBlank(in.readUTF());
  }

  protected void readGemFireVersion(final DataInput in) throws IOException {
    this.gemfireVersion = StringUtils.defaultIfBlank(in.readUTF());
  }

  protected void readJavaVersion(final DataInput in) throws IOException {
    this.javaVersion = StringUtils.defaultIfBlank(in.readUTF());
  }
  
  protected void readLogFile(final DataInput in) throws IOException {
    this.logFile = StringUtils.defaultIfBlank(in.readUTF());
  }
  
  protected void readHost(final DataInput in) throws IOException {
    this.host = StringUtils.defaultIfBlank(in.readUTF());
  }
  
  protected void readPort(final DataInput in) throws IOException {
    final int port = in.readInt();
    this.port = (port == 0 ? null : port);
  }
  
  protected void readName(final DataInput in) throws IOException {
    this.name = StringUtils.defaultIfBlank(in.readUTF());
  }

  @Override
  public void toData(final DataOutput out) throws IOException {
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
    out.writeInt(ObjectUtils.defaultIfNull(getPid(), 0));
  }

  protected void writeUptime(final DataOutput out) throws IOException {
    out.writeLong(getUptime());
  }

  protected void writeWorkingDirectory(final DataOutput out) throws IOException {
    out.writeUTF(ObjectUtils.defaultIfNull(getWorkingDirectory(), ""));
  }

  protected void writeJvmArguments(final DataOutput out) throws IOException {
    final List<String> jvmArgs = getJvmArgs();
    out.writeInt(jvmArgs.size());
    for (final String jvmArg : jvmArgs) {
      out.writeUTF(jvmArg);
    }
  }

  protected void writeClasspath(final DataOutput out) throws IOException {
    out.writeUTF(ObjectUtils.defaultIfNull(getClasspath(), ""));
  }

  protected void writeGemFireVersion(final DataOutput out) throws IOException {
    out.writeUTF(ObjectUtils.defaultIfNull(getGemFireVersion(), ""));
  }

  protected void writeJavaVersion(final DataOutput out) throws IOException {
    out.writeUTF(ObjectUtils.defaultIfNull(getJavaVersion(), ""));
  }
  
  protected void writeLogFile(final DataOutput out)throws IOException {
    out.writeUTF(ObjectUtils.defaultIfNull(getLogFile(), ""));
  }
  
  protected void writeHost(final DataOutput out)throws IOException {
    out.writeUTF(ObjectUtils.defaultIfNull(getHost(), ""));
  }
  
  protected void writePort(final DataOutput out) throws IOException {
    out.writeInt(ObjectUtils.defaultIfNull(getPort(), 0));
  }
  
  protected void writeName(final DataOutput out)throws IOException {
    out.writeUTF(ObjectUtils.defaultIfNull(getName(), ""));
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

    return ObjectUtils.equalsIgnoreNull(this.getPid(), that.getPid())
      && ObjectUtils.equals(this.getUptime(), that.getUptime())
      && ObjectUtils.equals(this.getWorkingDirectory(), that.getWorkingDirectory())
      && ObjectUtils.equals(this.getJvmArgs(), that.getJvmArgs())
      && ObjectUtils.equals(this.getClasspath(), that.getClasspath())
      && ObjectUtils.equals(this.getGemFireVersion(), that.getGemFireVersion())
      && ObjectUtils.equals(this.getJavaVersion(), that.getJavaVersion());
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
    final StringBuilder buffer = new StringBuilder(getClass().getSimpleName());
    buffer.append("{ pid = ").append(getPid());
    buffer.append(", uptime = ").append(getUptime());
    buffer.append(", workingDirectory = ").append(getWorkingDirectory());
    buffer.append(", jvmArgs = ").append(getJvmArgs());
    buffer.append(", classpath = ").append(getClasspath());
    buffer.append(", gemfireVersion = ").append(getGemFireVersion());
    buffer.append(", javaVersion = ").append(getJavaVersion());
    buffer.append("}");
    return buffer.toString();
  }

}
