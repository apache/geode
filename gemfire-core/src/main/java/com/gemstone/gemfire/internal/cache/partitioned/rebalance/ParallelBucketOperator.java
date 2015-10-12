package com.gemstone.gemfire.internal.cache.partitioned.rebalance;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

/**
 * A bucket operator that will perform operations on a bucket asynchronously.
 * 
 * This class wraps a delegate bucket operator that is synchronous. That is, the
 * delegate bucket operator is expected to move the bucket and notify the
 * Completion within the scope of the call to create bucket.
 * 
 * What this class does in make that call asynchronous. A task to create the
 * bucket is handed to the thread pool and executed there. After it is done, the
 * completion is notified.
 * 
 * Calling waitForOperations waits for all previously submitted operations and 
 * ensures the completions are notified.
 * 
 * Note that only createRedundantBucket is asynchronous, the rest of the 
 * operations are synchronous.
 * 
 */
public class ParallelBucketOperator implements BucketOperator {

  private final BucketOperator delegate;
  private final ExecutorService executor;
  private final Semaphore operationSemaphore;
  private final int maxParallelOperations;
  private final ConcurrentLinkedQueue<Completion> pendingSuccess = new ConcurrentLinkedQueue<BucketOperator.Completion>();
  private final ConcurrentLinkedQueue<Completion> pendingFailure = new ConcurrentLinkedQueue<BucketOperator.Completion>();
  

  /**
   * Create a parallel bucket operator
   * @param maxParallelOperations The number of operations that can execute concurrently. Futher calls to createRedundantBucket will block.
   * @param executor the executor to submit tasks to. This executor should be able to create at least maxParallelOperations threads.
   * @param operator A bucket operator that is synchronous that will do the actual work of creating a bucket.
   */
  public ParallelBucketOperator(int maxParallelOperations, ExecutorService executor, BucketOperator operator) {
    this.maxParallelOperations = maxParallelOperations;
    this.operationSemaphore = new Semaphore(maxParallelOperations);
    this.delegate = operator;
    this.executor = executor;
  }

  /**
   * Create a redundant bucket asynchronously. If maxParallelOperations is not reached, this call will submit 
   * a task and return immediately. Otherwise, it will block until an executor thread is available to 
   * take a task.
   * 
   * The completion will not be notified until the caller makes another call to createRedundant bucket or
   * waitForOperations.
   */
  @Override
  public void createRedundantBucket(final InternalDistributedMember targetMember,
      final int bucketId, final Map<String, Long> colocatedRegionBytes,
      final Completion completion) {
    drainCompletions();
    operationSemaphore.acquireUninterruptibly();
    executor.execute(new Runnable() {
      @Override
      public void run() {
        try {
          delegate.createRedundantBucket(targetMember, bucketId, colocatedRegionBytes, new Completion() {
            @Override
            public void onSuccess() {
              pendingSuccess.add(completion);
            }
            
            @Override
            public void onFailure() {
              pendingFailure.add(completion);
            }
          });
        } catch(CancelException e) {
          //ignore 
        } catch(RegionDestroyedException e) {
          //ignore
        } finally {
          operationSemaphore.release();
        }
      }
    });
    
  }

  @Override
  public boolean removeBucket(InternalDistributedMember memberId, int id,
      Map<String, Long> colocatedRegionSizes) {
    return delegate.removeBucket(memberId, id, colocatedRegionSizes);
  }

  @Override
  public boolean moveBucket(InternalDistributedMember sourceMember,
      InternalDistributedMember targetMember, int bucketId,
      Map<String, Long> colocatedRegionBytes) {
      return delegate.moveBucket(sourceMember, targetMember, bucketId, colocatedRegionBytes);
  }

  @Override
  public boolean movePrimary(InternalDistributedMember source,
      InternalDistributedMember target, int bucketId) {
    return delegate.movePrimary(source, target, bucketId);
  }
  
  public void drainCompletions() {
    Completion next = null;
    while((next = pendingSuccess.poll()) != null) {
      next.onSuccess();
    }
    
    while((next = pendingFailure.poll()) != null) {
      next.onFailure();
    }
    
  }

  /**
   * Wait for any pending operations, and notify the the completions
   * that the operations and done.
   */
  public void waitForOperations() {
    boolean interrupted = false;
    while(!executor.isShutdown()) {
      try {
        if(operationSemaphore.tryAcquire(maxParallelOperations, 1, TimeUnit.SECONDS)) {
          operationSemaphore.release(maxParallelOperations);
          
          drainCompletions();
          
          if(interrupted) {
            Thread.currentThread().interrupt();
          }
          
          return; 
        }
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
  }

}
