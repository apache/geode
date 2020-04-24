package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.management.internal.i18n.CliStrings.CLEAR_REGION;
import static org.apache.geode.management.internal.i18n.CliStrings.CLEAR_REGION_REGION_NAME;
import static org.apache.geode.management.internal.cli.commands.ClearCommand.REGION_NOT_FOUND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.domain.DataCommandRequest;
import org.apache.geode.management.internal.cli.domain.DataCommandResult;
import org.apache.geode.management.internal.cli.functions.DataCommandFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class ClearCommandTest {

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  static final String regionName = "regionName";
  static final String success = "SUCCESS";

  InternalCache cache;
  ClearCommand command;
  Region region;
  Set<DistributedMember> membersList;
  DistributedMember member;
  DataCommandResult dataResult;

  @Before
  public void setup() {
    cache = mock(InternalCache.class);
    command = spy(new ClearCommand());
    region = mock(Region.class);
    dataResult = mock(DataCommandResult.class);

    membersList = new HashSet<DistributedMember>();
    membersList.add(member);

    doNothing().when(command).authorize(any(), any(), anyString());
    doReturn(cache).when(command).getCache();
    doReturn(membersList).when(command).findAnyMembersForRegion(anyString());

    ResultModel result = ResultModel.createInfo(success);
    doReturn(result).when(dataResult).toResultModel();
  }

  @Test
  public void commandReturnsErrorIfRegionIsNotFound() {
    membersList.clear();

    gfsh.executeAndAssertThat(command, CLEAR_REGION + " --" + CLEAR_REGION_REGION_NAME + "=/" + regionName)
        .statusIsError().containsOutput(String.format(REGION_NOT_FOUND, "/" + regionName));
  }

  @Test
  public void commandReturnsSuccessfullyIfRegionIsFoundOnServersButNotLocator() {
    doReturn(dataResult).when(command).callFunctionForRegion(any(), any(), any());

    gfsh.executeAndAssertThat(command, CLEAR_REGION + " --" + CLEAR_REGION_REGION_NAME + "=/" + regionName)
        .statusIsSuccess().containsOutput(success);
  }

  @Test
  public void commandReturnsSuccessfullyIfRegionIsFoundOnLocator() {
    DataCommandFunction dataCommandFunction = mock(DataCommandFunction.class);
    doReturn(dataCommandFunction).when(command).createCommandFunction();
    when(cache.getRegion("/" + regionName)).thenReturn(region);

    doReturn(dataResult).when(dataCommandFunction)
        .remove(null, null, "/" + regionName, "ALL", cache);

    gfsh.executeAndAssertThat(command, CLEAR_REGION + " --" + CLEAR_REGION_REGION_NAME + "=/" + regionName)
        .statusIsSuccess().containsOutput(success);
  }
}
