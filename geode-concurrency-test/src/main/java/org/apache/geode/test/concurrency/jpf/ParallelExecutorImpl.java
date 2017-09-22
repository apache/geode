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
package org.apache.geode.test.concurrency.jpf;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.geode.test.concurrency.ParallelExecutor;
import org.apache.geode.test.concurrency.RunnableWithException;

class ParallelExecutorImpl implements ParallelExecutor {
  List<ThreadFuture<?>> futures = new ArrayList<>();

  @Override
  public <T> Future<T> inParallel(Callable<T> callable) {
    ThreadFuture<T> future = newThread(callable);
    futures.add(future);
    return future;
  }

  @Override
  public void execute() throws ExecutionException, InterruptedException {
    for (ThreadFuture future : futures) {
      future.start();
    }
    for (ThreadFuture future : futures) {
      future.get();
    }
  }


  private static <T> ThreadFuture<T> newThread(Callable<T> callable) {
    ThreadFuture<T> future = new ThreadFuture<T>(callable);
    Thread thread = new Thread(future);
    future.setThread(thread);
    return future;
  }

  private static class ThreadFuture<T> extends FutureTask<T> {

    private Thread thread;

    public ThreadFuture(Callable<T> callable) {
      super(callable);
    }

    public void setThread(Thread thread) {
      this.thread = thread;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
      thread.join();
      return super.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      thread.join(unit.toMillis(timeout));
      return super.get(0, TimeUnit.MILLISECONDS);
    }

    public void start() {
      thread.start();
    }
  }
}
