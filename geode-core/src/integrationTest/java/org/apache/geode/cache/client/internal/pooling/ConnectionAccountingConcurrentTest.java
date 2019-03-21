package org.apache.geode.cache.client.internal.pooling;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.test.concurrency.ConcurrentTestRunner;
import org.apache.geode.test.concurrency.ParallelExecutor;
import org.apache.geode.test.concurrency.RunnableWithException;
import org.apache.geode.test.concurrency.loop.LoopRunnerConfig;

@RunWith(ConcurrentTestRunner.class)
@LoopRunnerConfig(count = 10000)
public class ConnectionAccountingConcurrentTest {

  @Test
  public void tryCreateStaysWithinMax(ParallelExecutor executor) throws ExecutionException, InterruptedException {
    int max = 2;
    ConnectionAccounting accountant = new ConnectionAccounting(1, max);

    RunnableWithException tryCreate = () -> {
      if (accountant.tryCreate()) {
        assertThat(accountant.getCount()).isLessThanOrEqualTo(max).isGreaterThan(0);
        accountant.cancelTryCreate();
      }
    };

    executor.inParallel(tryCreate);
    executor.inParallel(tryCreate);
    executor.inParallel(tryCreate);

    executor.execute();

    assertThat(accountant.getCount()).isEqualTo(0);
  }

  @Test
  public void tryDestroyNeverGoesBelowMax(ParallelExecutor executor) throws ExecutionException, InterruptedException {
    int max = 2;
    int overfillMax = 4;
    ConnectionAccounting accountant = new ConnectionAccounting(1, max);

    // prefill
    accountant.create();
    accountant.create();
    accountant.create();
    accountant.create();

    RunnableWithException tryDestroy = () -> {
      if (accountant.tryDestroy()) {
        assertThat(accountant.getCount()).isGreaterThanOrEqualTo(max).isLessThanOrEqualTo(overfillMax);
        accountant.cancelTryDestroy();
      }
    };

    executor.inParallel(tryDestroy);
    executor.inParallel(tryDestroy);
    executor.inParallel(tryDestroy);

    executor.execute();

    assertThat(accountant.getCount()).isEqualTo(overfillMax);
  }
}