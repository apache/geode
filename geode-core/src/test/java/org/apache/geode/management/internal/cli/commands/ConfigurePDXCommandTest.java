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
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.internal.cache.CacheConfig.DEFAULT_PDX_IGNORE_UNREAD_FIELDS;
import static org.apache.geode.internal.cache.CacheConfig.DEFAULT_PDX_PERSISTENT;
import static org.apache.geode.internal.cache.CacheConfig.DEFAULT_PDX_READ_SERIALIZED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalClusterConfigurationService;
import org.apache.geode.internal.cache.CacheConfig;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.GfshParserRule;

@Category(UnitTest.class)
public class ConfigurePDXCommandTest {
  private static final String BASE_COMMAND_STRING = "configure pdx ";

  @ClassRule
  public static CustomGfshParserRule gfshParserRule = new CustomGfshParserRule();

  private InternalCache cache;
  private XmlEntity xmlEntity;
  private CacheCreation cacheCreation;
  private ConfigurePDXCommand command;
  private InternalClusterConfigurationService clusterConfigurationService;

  @Before
  public void setUp() throws Exception {
    cache = mock(InternalCache.class);
    xmlEntity = mock(XmlEntity.class);
    command = spy(ConfigurePDXCommand.class);
    cacheCreation = spy(CacheCreation.class);
    clusterConfigurationService = mock(InternalClusterConfigurationService.class);

    doReturn(cache).when(command).getCache();
    doReturn(xmlEntity).when(command).createXmlEntity(any());
    doReturn(cacheCreation).when(command).getCacheCreation(anyBoolean());
    doReturn(Collections.emptySet()).when(command).getAllNormalMembers();
    doReturn(clusterConfigurationService).when(command).getSharedConfiguration();
  }

  @Test
  public void parsingShouldSucceedWithoutArguments() {
    assertThat(gfshParserRule.parse(BASE_COMMAND_STRING)).isNotNull();
  }

  @Test
  public void parsingAutoCompleteShouldSucceed() throws Exception {
    GfshParserRule.CommandCandidate candidate = gfshParserRule.complete(BASE_COMMAND_STRING);

    assertThat(candidate).isNotNull();
    assertThat(candidate.getCandidates()).isNotNull();
    assertThat(candidate.getCandidates().size()).isEqualTo(5);
  }

  @Test
  public void executionShouldHandleInternalFailures() {
    doThrow(new RuntimeException("Can't create CacheCreation.")).when(command)
        .getCacheCreation(anyBoolean());
    gfshParserRule.executeAndAssertThat(command, BASE_COMMAND_STRING).statusIsError()
        .containsOutput("Could not process command due to error.")
        .containsOutput("Can't create CacheCreation.");
    doReturn(cacheCreation).when(command).getCacheCreation(anyBoolean());

    doThrow(new RuntimeException("Can't find members.")).when(command).getAllNormalMembers();
    gfshParserRule.executeAndAssertThat(command, BASE_COMMAND_STRING).statusIsError()
        .containsOutput("Could not process command due to error.")
        .containsOutput("Can't find members.");
    doReturn(Collections.emptySet()).when(command).getAllNormalMembers();

    doThrow(new RuntimeException("Can't create XmlEntity.")).when(command).createXmlEntity(any());
    gfshParserRule.executeAndAssertThat(command, BASE_COMMAND_STRING).statusIsError()
        .containsOutput("Could not process command due to error.")
        .containsOutput("Can't create XmlEntity.");
    doReturn(xmlEntity).when(command).createXmlEntity(any());

    doThrow(new RuntimeException("Can't create ReflectionBasedAutoSerializer.")).when(command)
        .createReflectionBasedAutoSerializer(anyBoolean(), any());
    gfshParserRule
        .executeAndAssertThat(command,
            BASE_COMMAND_STRING + "--auto-serializable-classes=" + new String[] {})
        .statusIsError().containsOutput("Could not process command due to error.")
        .containsOutput("Can't create ReflectionBasedAutoSerializer.");
    gfshParserRule
        .executeAndAssertThat(command,
            BASE_COMMAND_STRING + "--portable-auto-serializable-classes=" + new String[] {})
        .statusIsError().containsOutput("Could not process command due to error.")
        .containsOutput("Can't create ReflectionBasedAutoSerializer.");

    verify(command, times(0)).persistClusterConfiguration(any(), any());
  }

  @Test
  public void executionShouldFailIfBothPortableAndNonPortableClassesParametersAreSpecifiedAtTheSameTime() {
    gfshParserRule
        .executeAndAssertThat(command,
            BASE_COMMAND_STRING
                + "--auto-serializable-classes=org.apache.geode --portable-auto-serializable-classes=org.apache.geode")
        .statusIsError().containsOutput(
            "The autoserializer cannot support both portable and non-portable classes at the same time.");

    verify(command, times(0)).persistClusterConfiguration(any(), any());
  }

  @Test
  public void executionShouldIncludeWarningMessageWhenThereAreMembersAlreadyRunning() {
    Set<DistributedMember> members = new HashSet<>();
    DistributedMember mockMember = mock(DistributedMember.class);
    when(mockMember.getId()).thenReturn("member0");
    members.add(mockMember);
    doReturn(xmlEntity).when(command).createXmlEntity(any());
    doReturn(members).when(command).getAllNormalMembers();

    gfshParserRule.executeAndAssertThat(command, BASE_COMMAND_STRING).statusIsSuccess()
        .hasDefaultsConfigured(command, cacheCreation).containsOutput(
            "The command would only take effect on new data members joining the distributed system. It won't affect the existing data members");
  }

  @Test
  public void executionShouldWorkCorrectlyWhenDefaultsAreUsed() {
    // Factory Default
    gfshParserRule.executeAndAssertThat(command, BASE_COMMAND_STRING).statusIsSuccess()
        .hasNoFailToPersistError().hasDefaultsConfigured(command, cacheCreation);
  }

  @Test
  public void executionShouldCorrectlyConfigurePersistenceWhenDefaultDiskStoreIsUsed() {
    // Default Disk Store
    gfshParserRule.executeAndAssertThat(command, BASE_COMMAND_STRING + "--disk-store")
        .statusIsSuccess().hasNoFailToPersistError()
        .hasReadSerializedConfigured(DEFAULT_PDX_READ_SERIALIZED, cacheCreation)
        .hasIgnoreUnreadFieldsConfigured(DEFAULT_PDX_IGNORE_UNREAD_FIELDS, cacheCreation)
        .hasPersistenceConfigured(true, "DEFAULT", cacheCreation);

    verify(cacheCreation, times(0)).setPdxSerializer(any());
    verify(command, times(1)).persistClusterConfiguration(any(), any());
    verify(command, times(0)).createReflectionBasedAutoSerializer(anyBoolean(), any());
  }

  @Test
  public void executionShouldCorrectlyConfigurePersistenceWhenCustomDiskStoreIsUsed() {
    // Custom Disk Store
    gfshParserRule.executeAndAssertThat(command, BASE_COMMAND_STRING + "--disk-store=myDiskStore")
        .statusIsSuccess().hasNoFailToPersistError()
        .hasReadSerializedConfigured(DEFAULT_PDX_READ_SERIALIZED, cacheCreation)
        .hasIgnoreUnreadFieldsConfigured(DEFAULT_PDX_IGNORE_UNREAD_FIELDS, cacheCreation)
        .hasPersistenceConfigured(true, "myDiskStore", cacheCreation);

    verify(cacheCreation, times(0)).setPdxSerializer(any());
    verify(command, times(1)).persistClusterConfiguration(any(), any());
    verify(command, times(0)).createReflectionBasedAutoSerializer(anyBoolean(), any());
  }

  @Test
  public void executionShouldCorrectlyConfigureReadSerializedWhenFlagIsSetAsTrue() {
    // Custom Configuration as True
    gfshParserRule.executeAndAssertThat(command, BASE_COMMAND_STRING + "--read-serialized=true")
        .statusIsSuccess().hasNoFailToPersistError()
        .hasReadSerializedConfigured(true, cacheCreation)
        .hasIgnoreUnreadFieldsConfigured(DEFAULT_PDX_IGNORE_UNREAD_FIELDS, cacheCreation)
        .hasPersistenceConfigured(DEFAULT_PDX_PERSISTENT, null, cacheCreation);

    verify(cacheCreation, times(0)).setPdxSerializer(any());
    verify(command, times(1)).persistClusterConfiguration(any(), any());
    verify(command, times(0)).createReflectionBasedAutoSerializer(anyBoolean(), any());
  }

  @Test
  public void executionShouldCorrectlyConfigureReadSerializedWhenFlagIsSetAsFalse() {
    // Custom Configuration as False
    gfshParserRule.executeAndAssertThat(command, BASE_COMMAND_STRING + "--read-serialized=false")
        .statusIsSuccess().hasNoFailToPersistError()
        .hasReadSerializedConfigured(false, cacheCreation)
        .hasIgnoreUnreadFieldsConfigured(DEFAULT_PDX_IGNORE_UNREAD_FIELDS, cacheCreation)
        .hasPersistenceConfigured(DEFAULT_PDX_PERSISTENT, null, cacheCreation);

    verify(cacheCreation, times(0)).setPdxSerializer(any());
    verify(command, times(1)).persistClusterConfiguration(any(), any());
    verify(command, times(0)).createReflectionBasedAutoSerializer(anyBoolean(), any());
  }

  @Test
  public void executionShouldCorrectlyConfigureIgnoreUnreadFieldsWhenFlagIsSetAsTrue() {
    // Custom Configuration as True
    gfshParserRule
        .executeAndAssertThat(command, BASE_COMMAND_STRING + "--ignore-unread-fields=true")
        .statusIsSuccess().hasNoFailToPersistError()
        .hasReadSerializedConfigured(DEFAULT_PDX_READ_SERIALIZED, cacheCreation)
        .hasIgnoreUnreadFieldsConfigured(true, cacheCreation)
        .hasPersistenceConfigured(DEFAULT_PDX_PERSISTENT, null, cacheCreation);

    verify(cacheCreation, times(0)).setPdxSerializer(any());
    verify(command, times(1)).persistClusterConfiguration(any(), any());
    verify(command, times(0)).createReflectionBasedAutoSerializer(anyBoolean(), any());
  }

  @Test
  public void executionShouldCorrectlyConfigureIgnoreUnreadFieldsWhenFlagIsSetAsFalse() {
    // Custom Configuration as False
    gfshParserRule
        .executeAndAssertThat(command, BASE_COMMAND_STRING + "--ignore-unread-fields=false")
        .statusIsSuccess().hasNoFailToPersistError()
        .hasReadSerializedConfigured(DEFAULT_PDX_READ_SERIALIZED, cacheCreation)
        .hasIgnoreUnreadFieldsConfigured(false, cacheCreation)
        .hasPersistenceConfigured(DEFAULT_PDX_PERSISTENT, null, cacheCreation);

    verify(cacheCreation, times(0)).setPdxSerializer(any());
    verify(command, times(1)).persistClusterConfiguration(any(), any());
    verify(command, times(0)).createReflectionBasedAutoSerializer(anyBoolean(), any());
  }

  @Test
  public void executionShouldCorrectlyConfigurePortableAutoSerializableClassesWhenUsingCustomPattern() {
    String[] patterns = new String[] {"com.company.DomainObject.*#identity=id"};

    // Custom Settings
    gfshParserRule
        .executeAndAssertThat(command,
            BASE_COMMAND_STRING + "--portable-auto-serializable-classes=" + patterns[0])
        .statusIsSuccess().hasNoFailToPersistError()
        .hasReadSerializedConfigured(DEFAULT_PDX_READ_SERIALIZED, cacheCreation)
        .hasIgnoreUnreadFieldsConfigured(DEFAULT_PDX_IGNORE_UNREAD_FIELDS, cacheCreation)
        .hasPersistenceConfigured(DEFAULT_PDX_PERSISTENT, null, cacheCreation)
        .containsOutput("Portable Classes = [com.company.DomainObject.*#identity=id]")
        .containsOutput("PDX Serializer = org.apache.geode.pdx.ReflectionBasedAutoSerializer");

    verify(cacheCreation, times(1)).setPdxSerializer(any());
    verify(command, times(1)).persistClusterConfiguration(any(), any());
    verify(command, times(1)).createReflectionBasedAutoSerializer(true, patterns);
  }

  @Test
  public void executionShouldCorrectlyConfigureAutoSerializableClassesWhenUsingCustomPattern() {
    String[] patterns = new String[] {"com.company.DomainObject.*#identity=id"};

    // Custom Settings
    gfshParserRule
        .executeAndAssertThat(command,
            BASE_COMMAND_STRING + "--auto-serializable-classes=" + patterns[0])
        .statusIsSuccess().hasNoFailToPersistError()
        .hasReadSerializedConfigured(CacheConfig.DEFAULT_PDX_READ_SERIALIZED, cacheCreation)
        .hasIgnoreUnreadFieldsConfigured(CacheConfig.DEFAULT_PDX_IGNORE_UNREAD_FIELDS,
            cacheCreation)
        .hasPersistenceConfigured(CacheConfig.DEFAULT_PDX_PERSISTENT, null, cacheCreation)
        .containsOutput("Non Portable Classes = [com.company.DomainObject.*#identity=id]")
        .containsOutput("PDX Serializer = org.apache.geode.pdx.ReflectionBasedAutoSerializer");

    verify(cacheCreation, times(1)).setPdxSerializer(any());
    verify(command, times(1)).persistClusterConfiguration(any(), any());
    verify(command, times(1)).createReflectionBasedAutoSerializer(false, patterns);
  }

  static class CustomGfshParserRule extends GfshParserRule {
    @Override
    public <T> PDXCommandResultAssert executeAndAssertThat(T instance, String command) {
      CommandResultAssert resultAssert = super.executeAndAssertThat(instance, command);;

      return new PDXCommandResultAssert(resultAssert.getCommandResult());
    }
  }

  static class PDXCommandResultAssert extends CommandResultAssert {
    public PDXCommandResultAssert(CommandResult commandResult) {
      super(commandResult);
    }

    @Override
    public PDXCommandResultAssert statusIsError() {
      super.statusIsError();

      return this;
    }

    @Override
    public PDXCommandResultAssert statusIsSuccess() {
      super.statusIsSuccess();

      return this;
    }

    @Override
    public PDXCommandResultAssert containsOutput(String... expectedOutputs) {
      super.containsOutput(expectedOutputs);

      return this;
    }

    @Override
    public PDXCommandResultAssert hasNoFailToPersistError() {
      super.hasNoFailToPersistError();

      return this;
    }

    public PDXCommandResultAssert hasPersistenceConfigured(boolean persistenceEnabled,
        String diskStoreName, CacheCreation cache) {
      assertThat(actual.getOutput()).contains("persistent = " + persistenceEnabled);

      if (StringUtils.isNotEmpty(diskStoreName)) {
        assertThat(actual.getOutput()).contains("disk-store = " + diskStoreName);
      }

      if (persistenceEnabled) {
        verify(cache, times(1)).setPdxPersistent(true);
      } else {
        verify(cache, times(0)).setPdxPersistent(true);
      }

      return this;
    }


    public PDXCommandResultAssert hasReadSerializedConfigured(boolean readSerializedEnabled,
        CacheCreation cache) {
      assertThat(actual.getOutput()).contains("read-serialized = " + readSerializedEnabled);
      verify(cache, times(1)).setPdxReadSerialized(readSerializedEnabled);

      return this;
    }

    public PDXCommandResultAssert hasIgnoreUnreadFieldsConfigured(boolean ignoreUnreadFieldsEnabled,
        CacheCreation cache) {
      assertThat(actual.getOutput())
          .contains("ignore-unread-fields = " + ignoreUnreadFieldsEnabled);
      verify(cache, times(1)).setPdxIgnoreUnreadFields(ignoreUnreadFieldsEnabled);

      return this;
    }

    public PDXCommandResultAssert hasDefaultsConfigured(ConfigurePDXCommand command,
        CacheCreation cacheCreation) {
      hasNoFailToPersistError();
      hasReadSerializedConfigured(DEFAULT_PDX_READ_SERIALIZED, cacheCreation);
      hasIgnoreUnreadFieldsConfigured(DEFAULT_PDX_IGNORE_UNREAD_FIELDS, cacheCreation);
      hasPersistenceConfigured(DEFAULT_PDX_PERSISTENT, null, cacheCreation);

      verify(cacheCreation, times(0)).setPdxSerializer(any());
      verify(command, times(1)).persistClusterConfiguration(any(), any());
      verify(command, times(0)).createReflectionBasedAutoSerializer(anyBoolean(), any());

      return this;
    }
  }
}
