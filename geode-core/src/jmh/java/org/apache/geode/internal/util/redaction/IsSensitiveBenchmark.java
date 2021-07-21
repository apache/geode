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
package org.apache.geode.internal.util.redaction;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.internal.util.ArgumentRedactor.isSensitive;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@Measurement(iterations = 1, time = 1, timeUnit = MINUTES)
@Warmup(iterations = 1, time = 1, timeUnit = MINUTES)
@Fork(1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(SECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused")
public class IsSensitiveBenchmark {

  private static final String GEMFIRE_SECURITY_PASSWORD = "gemfire.security-password";
  private static final String PASSWORD = "password";
  private static final String OTHER_PASSWORD_OPTION = "other-password-option";
  private static final String CLUSTER_SSL_TRUSTSTORE_PASSWORD_PROPERTY =
      CLUSTER_SSL_TRUSTSTORE_PASSWORD;
  private static final String GATEWAY_SSL_TRUSTSTORE_PASSWORD_PROPERTY =
      GATEWAY_SSL_TRUSTSTORE_PASSWORD;
  private static final String SERVER_SSL_KEYSTORE_PASSWORD_PROPERTY = SERVER_SSL_KEYSTORE_PASSWORD;
  private static final String SECURITY_USERNAME = "security-username";
  private static final String SECURITY_MANAGER = "security-manager";
  private static final String SECURITY_IMPORTANT_PROPERTY = "security-important-property";
  private static final String JAVAX_NET_SSL_KEYSTOREPASSWORD = "javax.net.ssl.keyStorePassword";
  private static final String JAVAX_NET_SSL_SOME_SECURITY_ITEM = "javax.net.ssl.some.security.item";
  private static final String JAVAX_NET_SSL_KEYSTORETYPE = "javax.net.ssl.keyStoreType";
  private static final String SYSPROP_SECRET_PROP = "sysprop-secret-prop";

  private List<String> shouldBeRedacted;

  @Setup(Level.Trial)
  public void setUp() {
    shouldBeRedacted = asList(GEMFIRE_SECURITY_PASSWORD, PASSWORD, OTHER_PASSWORD_OPTION,
        CLUSTER_SSL_TRUSTSTORE_PASSWORD_PROPERTY, GATEWAY_SSL_TRUSTSTORE_PASSWORD_PROPERTY,
        SERVER_SSL_KEYSTORE_PASSWORD_PROPERTY, SECURITY_USERNAME, SECURITY_MANAGER,
        SECURITY_IMPORTANT_PROPERTY, JAVAX_NET_SSL_KEYSTOREPASSWORD, JAVAX_NET_SSL_KEYSTOREPASSWORD,
        JAVAX_NET_SSL_SOME_SECURITY_ITEM, JAVAX_NET_SSL_KEYSTORETYPE, SYSPROP_SECRET_PROP);

    for (String option : shouldBeRedacted) {
      assertThat(isSensitive(option))
          .describedAs("This option should be identified as taboo: " + option)
          .isTrue();
    }
  }

  @Benchmark
  public void isSensitive_GEMFIRE_SECURITY_PASSWORD_benchmark(Blackhole blackhole) {
    blackhole.consume(isSensitive(GEMFIRE_SECURITY_PASSWORD));
  }

  @Benchmark
  public void isSensitive_PASSWORD_benchmark(Blackhole blackhole) {
    blackhole.consume(isSensitive(PASSWORD));
  }

  @Benchmark
  public void isSensitive_OTHER_PASSWORD_OPTION_benchmark(Blackhole blackhole) {
    blackhole.consume(isSensitive(OTHER_PASSWORD_OPTION));
  }

  @Benchmark
  public void isSensitive_CLUSTER_SSL_TRUSTSTORE_PASSWORD_PROPERTY_benchmark(Blackhole blackhole) {
    blackhole.consume(isSensitive(CLUSTER_SSL_TRUSTSTORE_PASSWORD_PROPERTY));
  }
}
