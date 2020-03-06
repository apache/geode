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

package org.apache.geode.management.internal.cli.functions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.logging.Level;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.configuration.JndiBindingsType;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.internal.logging.LocalLogWriter;
import org.apache.geode.management.internal.cli.commands.CreateJndiBindingCommand;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.test.junit.categories.GfshTest;

@Category({GfshTest.class})
public class ListJndiBindingFunctionTest {

  private ListJndiBindingFunction listJndiBindingFunction;
  private FunctionContext<Void> context;
  private ResultSender<Object> resultSender;
  private ArgumentCaptor<CliFunctionResult> resultCaptor;

  @SuppressWarnings({"unchecked", "deprecation"})
  @Before
  public void setup() throws Exception {
    listJndiBindingFunction = spy(new ListJndiBindingFunction());
    context = mock(FunctionContext.class);
    DistributedSystem distributedSystem = mock(DistributedSystem.class);
    resultSender = mock(ResultSender.class);
    resultCaptor = ArgumentCaptor.forClass(CliFunctionResult.class);

    when(distributedSystem.getLogWriter()).thenReturn(new LocalLogWriter(Level.FINE.intValue()));

    JNDIInvoker.mapTransactions(distributedSystem);

    JndiBindingsType.JndiBinding config = new JndiBindingsType.JndiBinding();
    config.setJndiName("jndi1");
    config.setType(CreateJndiBindingCommand.DATASOURCE_TYPE.SIMPLE.getType());
    config.setJdbcDriverClass("org.apache.derby.jdbc.EmbeddedDriver");
    config.setConnectionUrl("jdbc:derby:newDB;create=true");
    config.getConfigProperties()
        .add(new JndiBindingsType.JndiBinding.ConfigProperty("test", "test", "test"));
    JNDIInvoker.mapDatasource(CreateJndiBindingFunction.getParamsAsMap(config),
        CreateJndiBindingFunction.convert(config.getConfigProperties()));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void listJndiBindingIsSuccessful() {
    when(context.getResultSender()).thenReturn(resultSender);

    listJndiBindingFunction.execute(context);

    verify(resultSender).lastResult(resultCaptor.capture());
    CliFunctionResult result = resultCaptor.getValue();

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getSerializables()).containsExactlyInAnyOrder("java:UserTransaction",
        "org.apache.geode.internal.jta.UserTransactionImpl", "java:TransactionManager",
        "org.apache.geode.internal.jta.TransactionManagerImpl", "java:jndi1",
        "org.apache.geode.internal.datasource.GemFireBasicDataSource");
  }

}
