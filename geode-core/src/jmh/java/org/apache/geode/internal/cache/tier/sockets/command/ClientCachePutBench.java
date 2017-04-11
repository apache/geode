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
package org.apache.geode.internal.cache.tier.sockets.command;

import static java.lang.System.*;
import static java.util.concurrent.TimeUnit.*;
import static org.apache.commons.io.FileUtils.*;
import static org.apache.commons.lang.StringUtils.*;
import static org.apache.geode.cache.client.ClientRegionShortcut.*;
import static org.apache.geode.distributed.AbstractLauncher.Status.*;
import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.apache.geode.distributed.internal.DistributionConfig.*;
import static org.apache.geode.internal.AvailablePort.*;
import static org.apache.geode.test.dunit.NetworkUtils.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.*;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.internal.process.ProcessStreamReader;
import org.junit.rules.TemporaryFolder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark that measures throughput of single-threaded client performing puts to a loner server.
 * <p>
 * 100 random keys and values are generated during setup and the client reuses these in order,
 * looping back around after 100 puts.
 */
@Measurement(iterations = 3, time = 2, timeUnit = MINUTES)
@Warmup(iterations = 1, time = 1, timeUnit = MINUTES)
@Fork(3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@SuppressWarnings("unused")
public class ClientCachePutBench {

  static final String CLASS_NAME = ClientCachePutBench.class.getSimpleName();
  static final String PACKAGE_NAME =
      replace(ClientCachePutBench.class.getPackage().getName(), ".", "/");
  static final String REGION_NAME = CLASS_NAME + "-region";
  static final String SERVER_XML_NAME = "/" + PACKAGE_NAME + "/" + CLASS_NAME + "-server.xml";
  static final long PROCESS_READER_TIMEOUT = 60 * 1000;
  static final int NUMBER_OF_KEYS = 100;
  static final int NUMBER_OF_VALUES = 100;

  @State(Scope.Benchmark)
  public static class ClientState {

    Region<String, String> region;

    String[] keys;
    String[] values;

    private int keyIndex = -1;
    private int valueIndex = -1;

    private Process process;
    private volatile ProcessStreamReader processOutReader;
    private volatile ProcessStreamReader processErrReader;

    private int serverPort;
    private ServerLauncher launcher;
    private File serverDirectory;
    private ClientCache clientCache;

    private TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Setup(Level.Trial)
    public void startServer() throws Exception {
      System.out.println("\n" + "[ClientCachePutBench] startServer");

      Random random = new Random(nanoTime());
      this.keys = createStrings("KEY-", NUMBER_OF_KEYS, random);
      this.values = createStrings("VAL-", NUMBER_OF_VALUES, random);

      this.temporaryFolder.create();
      this.serverDirectory = this.temporaryFolder.getRoot();

      this.serverPort = getRandomAvailablePort(SOCKET);
      this.process = startServerProcess(this.serverDirectory, this.serverPort);

      try {
        startProcessReaders(this.process);

        ServerLauncher serverLauncher = new ServerLauncher.Builder()
            .setWorkingDirectory(this.serverDirectory.getAbsolutePath()).build();

        await("Starting server in " + this.serverDirectory).atMost(2, MINUTES)
            .until(() -> assertThat(serverLauncher.status().getStatus()).isEqualTo(ONLINE));

        this.clientCache = new ClientCacheFactory().set(LOG_LEVEL, "warn")
            .addPoolServer(getIPLiteral(), this.serverPort).create();
        this.region =
            this.clientCache.<String, String>createClientRegionFactory(PROXY).create(REGION_NAME);

      } finally {
        stopProcessReaders(PROCESS_READER_TIMEOUT);
      }
    }

    private String[] createStrings(final String prefix, final int count, final Random random) {
      String[] strings = new String[count];
      for (int i = 0; i < count; i++) {
        strings[i] = new StringBuilder(prefix).append(random.nextInt()).toString();
      }
      return strings;
    }

    private Process startServerProcess(final File serverDirectory, final int serverPort)
        throws IOException {
      File destServerXml = copyXmlToServerDirectory(SERVER_XML_NAME, serverDirectory);

      List<String> command = new ArrayList<>();
      command.add(new File(new File(getProperty("java.home"), "bin"), "java").getCanonicalPath());
      command.add("-D" + GEMFIRE_PREFIX + CACHE_XML_FILE + "=" + destServerXml.getAbsolutePath());
      command.add("-D" + GEMFIRE_PREFIX + MCAST_PORT + "=0");
      command.add("-D" + GEMFIRE_PREFIX + LOCATORS + "=");
      command.add("-D" + GEMFIRE_PREFIX + LOG_LEVEL + "=warn");
      command.add("-cp");
      command.add(getProperty("java.class.path"));
      command.add(ServerLauncher.class.getName());
      command.add(ServerLauncher.Command.START.getName());
      command.add("server1");
      command.add("--server-port=" + serverPort);

      System.out.println("[ClientCachePutBench] Launching server with command: " + command);

      return new ProcessBuilder(command).directory(this.serverDirectory).start();
    }

    private File copyXmlToServerDirectory(final String serverXmlName, final File serverDirectory)
        throws IOException {
      URL srcServerXml = getClass().getResource(serverXmlName);
      assertThat(srcServerXml).isNotNull();
      File destServerXml = new File(serverDirectory, serverXmlName);
      copyURLToFile(srcServerXml, destServerXml);
      return destServerXml;
    }

    private void startProcessReaders(final Process process) {
      this.processOutReader =
          new ProcessStreamReader.Builder(process).inputStream(process.getInputStream())
              .inputListener((line) -> System.out.println("[ClientCachePutBench][stdout] " + line))
              .build().start();
      this.processErrReader =
          new ProcessStreamReader.Builder(process).inputStream(process.getErrorStream())
              .inputListener((line) -> System.out.println("[ClientCachePutBench][stderr] " + line))
              .build().start();
    }

    private void stopProcessReaders(final long timeoutMillis) throws InterruptedException {
      if (this.processOutReader != null) {
        this.processOutReader.stop().join(timeoutMillis);
      }
      if (this.processErrReader != null) {
        this.processErrReader.stop().join(timeoutMillis);
      }
    }

    @TearDown(Level.Trial)
    public void stopServer() throws Exception {
      System.out.println("\n" + "[ClientCachePutBench] stopServer");
      try {
        this.clientCache.close(false);
        new ServerLauncher.Builder().setWorkingDirectory(this.serverDirectory.getAbsolutePath())
            .build().stop();
      } finally {
        if (this.process != null) {
          this.process.destroyForcibly();
        }
        this.temporaryFolder.delete();
      }
    }

    String nextKey() {
      this.keyIndex++;
      if (this.keyIndex >= this.keys.length) {
        this.keyIndex = 0;
      }
      return this.keys[this.keyIndex];
    }

    String nextValue() {
      this.valueIndex++;
      if (this.valueIndex >= this.values.length) {
        this.valueIndex = 0;
      }
      return this.values[this.valueIndex];
    }

  }

  @Benchmark
  public String performPutFromClient(final ClientState state) throws Exception {
    return state.region.put(state.nextKey(), state.nextValue());
  }

}
