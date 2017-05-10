package org.apache.geode.internal.cache.wan.asyncqueue;

import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.distributed.DistributedMember;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by dan on 5/10/17.
 */
public abstract class AbstractMovingAsyncEventListener implements AsyncEventListener {
  protected final DistributedMember destination;
  boolean moved;
  Set<Object> keysSeen = new HashSet<Object>();

  public AbstractMovingAsyncEventListener(
      final DistributedMember destination) {
    this.destination = destination;
  }

  @Override
  public boolean processEvents(final List<AsyncEvent> events) {
    if (!moved) {

      AsyncEvent event1 = events.get(0);
      move(event1);
      moved = true;
      return false;
    }

    events.stream().map(AsyncEvent::getKey).forEach(keysSeen::add);
    return true;
  }

  protected abstract void move(AsyncEvent event1);

  @Override
  public void close() {

  }
}
