package org.apache.geode.management.internal.cli.functions;


import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

public class RegionFunctionArgsTest {

  private RegionFunctionArgs args;
  private RegionFunctionArgs.PartitionArgs partitionArgs;

  @Before
  public void before() {
    args = new RegionFunctionArgs();
    partitionArgs = new RegionFunctionArgs.PartitionArgs();
  }

  @Test
  public void defaultRegionFunctionArgs() throws Exception {
    assertThat(args.isDiskSynchronous()).isNull();
    assertThat(args.isCloningEnabled()).isNull();
    assertThat(args.isConcurrencyChecksEnabled()).isNull();
    assertThat(args.getConcurrencyLevel()).isNull();
    assertThat(args.getPartitionArgs()).isNotNull();
    assertThat(args.getPartitionArgs().hasPartitionAttributes()).isFalse();
  }

  @Test
  public void defaultPartitionArgs() throws Exception {
    assertThat(partitionArgs.hasPartitionAttributes()).isFalse();

    partitionArgs.setPartitionResolver(null);
    assertThat(partitionArgs.hasPartitionAttributes()).isFalse();

    partitionArgs.setPrTotalNumBuckets(10);
    assertThat(partitionArgs.getPrTotalNumBuckets()).isEqualTo(10);
    assertThat(partitionArgs.hasPartitionAttributes()).isTrue();
  }
}
