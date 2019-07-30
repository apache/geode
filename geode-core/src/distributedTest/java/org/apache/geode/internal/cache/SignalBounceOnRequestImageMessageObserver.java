package org.apache.geode.internal.cache;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;

import java.util.concurrent.TimeoutException;

import org.apache.geode.cache.Cache;
import org.apache.geode.test.dunit.DUnitBlackboard;
import org.apache.geode.test.dunit.VM;

public class SignalBounceOnRequestImageMessageObserver extends OnRequestImageMessageObserver {
  private static final String gateName = "bounce";

  public SignalBounceOnRequestImageMessageObserver(String regionName, Cache cache,
      DUnitBlackboard dUnitBlackboard) {
    super(regionName, () -> {
      dUnitBlackboard.signalGate(gateName);
      await().until(() -> cache.isClosed());
    });
  }

  public static void waitThenBounce(DUnitBlackboard dUnitBlackboard, VM serverVM)
      throws TimeoutException, InterruptedException {
    dUnitBlackboard.waitForGate(gateName, 30, SECONDS);
    serverVM.bounceForcibly();
  }
}
