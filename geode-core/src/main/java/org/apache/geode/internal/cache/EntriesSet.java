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

package org.apache.geode.internal.cache;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.LocalRegion.IteratorType;
import org.apache.geode.internal.cache.entries.AbstractRegionEntry;
import org.apache.geode.logging.internal.log4j.api.LogWithToString;

/** Set view of entries */
public class EntriesSet extends AbstractSet implements LogWithToString {

  final LocalRegion topRegion;

  final boolean recursive;

  final IteratorType iterType;

  protected final TXStateInterface myTX;

  final boolean allowTombstones;

  protected final InternalDataView view;

  final boolean rememberReads;

  private boolean keepSerialized = false;

  protected boolean ignoreCopyOnReadForQuery = false;

  EntriesSet(LocalRegion region, boolean recursive, IteratorType viewType,
      boolean allowTombstones) {
    topRegion = region;
    this.recursive = recursive;
    iterType = viewType;
    myTX = region.getTXState();
    view = myTX == null ? region.getSharedDataView() : myTX;
    rememberReads = true;
    this.allowTombstones = allowTombstones;
  }

  protected void checkTX() {
    if (myTX != null) {
      if (!myTX.isInProgress()) {
        throw new IllegalStateException(
            String.format(
                "Region collection was created with transaction %s that is no longer active.",
                myTX.getTransactionId()));
      }
    } else {
      if (topRegion.isTX()) {
        throw new IllegalStateException(
            String.format(
                "The Region collection is not transactional but is being used in a transaction %s.",
                topRegion.getTXState().getTransactionId()));
      }
    }
  }

  @Override
  public Iterator<Object> iterator() {
    checkTX();
    return new EntriesIterator();
  }

  private class EntriesIterator implements Iterator<Object> {

    final List<LocalRegion> regions;

    final int numSubRegions;

    int regionsIndex;

    LocalRegion currRgn;

    // keep track of look-ahead on hasNext() call, used to filter out null
    // values
    Object nextElem;

    Iterator<?> currItr;

    Collection<?> additionalKeysFromView;

    /** reusable KeyInfo */
    protected final KeyInfo keyInfo = new KeyInfo(null, null, null);

    @SuppressWarnings("unchecked")
    protected EntriesIterator() {
      if (recursive) {
        // FIFO queue of regions
        regions = new ArrayList<LocalRegion>(topRegion.subregions(true));
        numSubRegions = regions.size();
      } else {
        regions = null;
        numSubRegions = 0;
      }
      createIterator(topRegion);
      nextElem = moveNext();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException(
          "This iterator does not support modification");
    }

    @Override
    public boolean hasNext() {
      return (nextElem != null);
    }

    @Override
    public Object next() {
      final Object result = nextElem;
      if (result != null) {
        nextElem = moveNext();
        return result;
      }
      throw new NoSuchElementException();
    }

    private Object moveNext() {
      // keep looping until:
      // we find an element and return it
      // OR we run out of elements and return null
      for (;;) {
        if (currItr.hasNext()) {
          final Object currKey = currItr.next();
          final Object result;

          keyInfo.setKey(currKey);
          if (additionalKeysFromView != null) {
            if (currKey instanceof AbstractRegionEntry) {
              additionalKeysFromView.remove(((AbstractRegionEntry) currKey).getKey());
            } else {
              additionalKeysFromView.remove(currKey);
            }
          }
          if (iterType == IteratorType.KEYS) {
            result =
                view.getKeyForIterator(keyInfo, currRgn, rememberReads, allowTombstones);
            if (result != null) {
              return result;
            }
          } else if (iterType == IteratorType.ENTRIES) {
            result = view.getEntryForIterator(keyInfo, currRgn, rememberReads,
                allowTombstones);
            if (result != null) {
              return result;
            }
          } else {
            Region.Entry re = (Region.Entry) view.getEntryForIterator(keyInfo, currRgn,
                rememberReads, allowTombstones);
            if (re != null) {
              try {
                if (keepSerialized) {
                  result = ((NonTXEntry) re).getRawValue(); // OFFHEAP: need to either copy into a
                                                            // cd or figure out when result will be
                                                            // released.
                } else if (ignoreCopyOnReadForQuery) {
                  result = ((NonTXEntry) re).getValue(true);
                } else {
                  if ((re instanceof TXEntry)) {
                    result = ((TXEntry) re).getValue(allowTombstones);
                  } else {
                    result = re.getValue();
                  }
                }
                if (result != null && !Token.isInvalidOrRemoved(result)) { // fix for bug 34583
                  return result;
                }
                if (result == Token.TOMBSTONE && allowTombstones) {
                  return result;
                }
              } catch (EntryDestroyedException ede) {
                // Fix for bug 43526, caused by fix to 43064
                // Entry is destroyed, continue to the next element.
              }
            }
            // key disappeared or is invalid, go on to next
          }
        } else if (additionalKeysFromView != null) {
          currItr = additionalKeysFromView.iterator();
          additionalKeysFromView = null;
        } else if (regionsIndex < numSubRegions) {
          // advance to next region
          createIterator(regions.get(regionsIndex));
          ++regionsIndex;
        } else {
          return null;
        }
      }
    }

    private void createIterator(final LocalRegion rgn) {
      // TX iterates over KEYS.
      // NonTX iterates over RegionEntry instances
      currRgn = rgn;
      currItr = view.getRegionKeysForIteration(rgn).iterator();
      additionalKeysFromView = view.getAdditionalKeysForIterator(rgn);
    }
  }

  @Override
  public int size() {
    checkTX();
    if (iterType == IteratorType.VALUES) {
      // if this is a values-view, then we have to filter out nulls to
      // determine the correct size
      int s = 0;
      for (Iterator<Object> itr = new EntriesIterator(); itr.hasNext(); itr.next()) {
        s++;
      }
      return s;
    } else if (recursive) {
      return topRegion.allEntriesSize();
    } else {
      return view.entryCount(topRegion);
    }
  }

  @Override
  public Object[] toArray() {
    return toArray(null);
  }

  @Override
  public Object[] toArray(final Object[] array) {
    checkTX();
    final ArrayList<Object> temp = new ArrayList<Object>(size());
    final Iterator<Object> iter = new EntriesIterator();
    while (iter.hasNext()) {
      temp.add(iter.next());
    }
    if (array == null) {
      return temp.toArray();
    } else {
      return temp.toArray(array);
    }
  }


  public void setKeepSerialized(boolean keepSerialized) {
    this.keepSerialized = keepSerialized;
  }


  public boolean isKeepSerialized() {
    return keepSerialized;
  }

  public void setIgnoreCopyOnReadForQuery(boolean ignoreCopyOnReadForQuery) {
    this.ignoreCopyOnReadForQuery = ignoreCopyOnReadForQuery;
  }

  public boolean isIgnoreCopyOnReadForQuery() {
    return ignoreCopyOnReadForQuery;
  }

}
