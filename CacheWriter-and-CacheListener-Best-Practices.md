GemFire provides APIs to capture a handful of events in a distributed system providing extensive documentation on how to implement listeners and callbacks for processing those synchronously or asynchronously.

This document covers `CacheWriter` and `CacheListener` best practices for handling `CacheEvents`.

## Event Model

![Event Model](https://wmarkito.files.wordpress.com/2015/03/07-events_pptx.png)

## Cache Writers

A `CacheWriter` is an event handler that synchronously handles “_happens-before_” events (`CacheEvent`) in an ordered fashion. It is mostly used for data validation and in some cases data synchronization with external data sources, offering a write-through capability for regions handling events that can be local, within the same JVM, or remote, in the case of replicated or partitioned region.

#### Basic rules:

* There can be only **one** `CacheWriter` per Region.
* For partitioned regions, only the writer in the event’s primary node will process the event.
* For replicated regions, only the first node to successfully execute the writer will process the event.
* For local regions, only local writer (if defined) will process the event.
* Unlike cache listeners, you can only install one cache writer in a region for each member.
* `CacheWriter` can abort operations (fail-fast) and will propagate the `CacheWriterException` back to the caller.
* Being a synchronous callback, it will block the current execution until it completes.

Available events and callbacks:

    beforeCreate(EntryEvent event)- Invoked before an entry is created.
    beforeUpdate(EntryEvent event) - Invoked before an entry is updated.
    beforeDestroy(EntryEvent event) - Invoked before an entry is destroyed.
    beforeRegionClear(RegionEvent event) - Invoked before a region is cleared.
    beforeRegionDestroy(RegionEvent event) - Invoked before a region is destroyed.

It is not recommended to perform long running operations inside a `CacheWriter` within the current thread. If such a long running operation is needed consider processing it asynchronously through an `AsyncEventListener` for example. Using an `ExecutorService` to delegate that execution to a different thread is possible, but considered an anti-pattern, since it breaks the fail-fast and happens-before concept of this callback.

## Cache Listeners

A `CacheListener` is an event handler that synchronously responds to events after modifications occurred in the system. The main use cases for a `CacheListener` are synchronous write-behind and notifications, triggering actions after certain modifications occur on a region or on the system. It can handle cache events related to entries (`EntryEvent`) and regions (`RegionEvent`) but events can be processed in a different order than the order they’re applied in the region.

#### Basic rules:

* You can install **multiple** `CacheListener` in the same region.
* When multiple listeners are installed, they will be executed in the same order they’re registered in the system. One at a time.
* For partitioned regions, only the listeners in the event’s primary node will process the event.
* For replicated regions, all nodes with the listener installed will process the event.
* For local regions, only local listener (if defined) will process the event.
* For long running or batch processing consider using an `AsynchronousEventListener`.
* Operations inside a `CacheWriter` are thread-safe and entries are locked for the current thread.

Available events and callbacks:

    afterCreate(EntryEvent<K,V> event) - Handles the event of new key being added to a region
    afterDestroy(EntryEvent<K,V> event) - Handles the event of an entry being destroyed
    afterInvalidate(EntryEvent<K,V> event) - Handles the event of an entry's value being invalidated
    afterRegionClear(RegionEvent<K,V> event) - Handles the event of a region being cleared
    afterRegionCreate(RegionEvent<K,V> event) - Handles the event of a region being created
    afterRegionDestroy(RegionEvent<K,V> event) - Handles the event of a region being destroyed
    afterRegionInvalidate(RegionEvent<K,V> event) - Handles the event of a region being invalidated
    afterRegionLive(RegionEvent<K,V> event) - Handles the event of a region being live after receiving the marker from the server
    afterUpdate(EntryEvent<K,V> event) - Handles the event of an entry's value being modified in a region

## General recommendations

When dealing with GemFire callbacks there are some operations that should be avoided or used with extra attention. They are:

* Do not perform distributed operations, such as using using the _Distributed Lock service_ 
* Avoid calling Region methods, particularly on non-colocated partitioned regions.
* Avoid calling functions through `FunctionService`, since they can cause distributed deadlocks.
* Do not use any GemFire APIs inside a `CacheWriter` if you have _conserve-sockets_ set to true.
* Do not modify region attributes, since those messages will have priority and can cause blocks.
* Avoid configurations where listeners or writers are deployed in a few nodes of the distributed system. Prefer a cluster-wide installation where every node can process the callback.
* Operations inside a `CacheListener` are thread-safe and entries are locked for the current thread.

When using transactions:
* `CacheWriter` should not start transactions;
* `CacheWriter` and `CacheListener` will receive all individual operations part of a transaction, unlike their transactional counterparts `TransactionWriter` and `TransactionListener`;
* When a rollback or commit happens, a `CacheWriter` can only be notified by a `TransactionWriter` and should handle rollback or failures properly;
* `CacheWriterException` is still propagated to the application and it should handle the failures in the context of the transaction by continuing or aborting;  JTA is the recommended alternative
* In most cases when dealing with transactions, consider using a `TransactionWriter` instead of a `CacheWriter`
* With global transactions, `EntryEvent.getTransactionId()` will return the current internal transaction ID
* Use the same transactional data source and make sure it’s JTA enabled, so database operations inside a `CacheWriter` can be rolled back and participate in the same global transaction

When dealing with transactions always consider using `TransactionListener` or `TransactionWriter` for handling transaction events, but do notice that they’re cache-wide handlers.

![Listener Model](https://wmarkito.files.wordpress.com/2015/03/gf_listener.png)