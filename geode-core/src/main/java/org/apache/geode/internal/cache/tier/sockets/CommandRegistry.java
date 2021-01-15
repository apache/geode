package org.apache.geode.internal.cache.tier.sockets;

import java.util.Map;

import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.serialization.KnownVersion;

public interface CommandRegistry {
  void register(int messageType, Map<KnownVersion, Command> versionToNewCommand);

  Map<Integer, Command> get(KnownVersion version);
}
