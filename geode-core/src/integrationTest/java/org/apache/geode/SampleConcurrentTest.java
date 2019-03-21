package org.apache.geode;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.test.concurrency.ConcurrentTestRunner;
import org.apache.geode.test.concurrency.ParallelExecutor;
import org.apache.geode.test.concurrency.loop.LoopRunnerConfig;

@RunWith(ConcurrentTestRunner.class)
@LoopRunnerConfig(count = 100000)
public class SampleConcurrentTest {

  @Test
  public void test(ParallelExecutor executor) throws ExecutionException, InterruptedException {
    Map<Integer, Integer> map = new HashMap<>();
//   ConcurrentMap<Integer, Integer> map = new ConcurrentHashMap<>();

    executor.inParallel(() -> {
      map.put(1, 1);
      assertThat(map.get(1)).isEqualTo(1);
    });

    executor.inParallel(() -> {
      map.put(2, 2);
      assertThat(map.get(2)).isEqualTo(2);
    });

    executor.execute();

    assertThat(map.size()).isEqualTo(2);
  }
}
