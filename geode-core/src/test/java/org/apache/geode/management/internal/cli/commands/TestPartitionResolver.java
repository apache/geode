package org.apache.geode.management.internal.cli.commands;

import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.PartitionResolver;

public class TestPartitionResolver implements PartitionResolver {
  @Override
  public Object getRoutingObject(EntryOperation opDetails) {
    return null;
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public void close() {

  }
}
