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
import org.apache.geode.internal.cache.LocalRegion.NonTXEntry;
import org.apache.geode.internal.cache.entries.AbstractRegionEntry;
import org.apache.geode.internal.i18n.LocalizedStrings;

/** Set view of entries */
public class EntriesSet extends AbstractSet {

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
    this.topRegion = region;
    this.recursive = recursive;
    this.iterType = viewType;
    this.myTX = region.getTXState();
    this.view = this.myTX == null ? region.getSharedDataView() : this.myTX;
    this.rememberReads = true;
    this.allowTombstones = allowTombstones;
  }

  protected void checkTX() {
    if (this.myTX != null) {
      if (!myTX.isInProgress()) {
        throw new IllegalStateException(
            LocalizedStrings.LocalRegion_REGION_COLLECTION_WAS_CREATED_WITH_TRANSACTION_0_THAT_IS_NO_LONGER_ACTIVE
                .toLocalizedString(myTX.getTransactionId()));
      }
    } else {
      if (this.topRegion.isTX()) {
        throw new IllegalStateException(
            LocalizedStrings.LocalRegion_NON_TRANSACTIONAL_REGION_COLLECTION_IS_BEING_USED_IN_A_TRANSACTION
                .toLocalizedString(this.topRegion.getTXState().getTransactionId()));
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
        this.regions = new ArrayList<LocalRegion>(topRegion.subregions(true));
        this.numSubRegions = this.regions.size();
      } else {
        this.regions = null;
        this.numSubRegions = 0;
      }
      createIterator(topRegion);
      this.nextElem = moveNext();
    }

    public void remove() {
      throw new UnsupportedOperationException(
          LocalizedStrings.LocalRegion_THIS_ITERATOR_DOES_NOT_SUPPORT_MODIFICATION
              .toLocalizedString());
    }

    public boolean hasNext() {
      return (this.nextElem != null);
    }

    public Object next() {
      final Object result = this.nextElem;
      if (result != null) {
        this.nextElem = moveNext();
        return result;
      }
      throw new NoSuchElementException();
    }

    private Object moveNext() {
      // keep looping until:
      // we find an element and return it
      // OR we run out of elements and return null
      for (;;) {
        if (this.currItr.hasNext()) {
          final Object currKey = this.currItr.next();
          final Object result;

          this.keyInfo.setKey(currKey);
          if (this.additionalKeysFromView != null) {
            if (currKey instanceof AbstractRegionEntry) {
              this.additionalKeysFromView.remove(((AbstractRegionEntry) currKey).getKey());
            } else {
              this.additionalKeysFromView.remove(currKey);
            }
          }
          if (iterType == IteratorType.KEYS) {
            result =
                view.getKeyForIterator(this.keyInfo, this.currRgn, rememberReads, allowTombstones);
            if (result != null) {
              return result;
            }
          } else if (iterType == IteratorType.ENTRIES) {
            result = view.getEntryForIterator(this.keyInfo, this.currRgn, rememberReads,
                allowTombstones);
            if (result != null) {
              return result;
            }
          } else {
            Region.Entry re = (Region.Entry) view.getEntryForIterator(this.keyInfo, currRgn,
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
                  result = re.getValue();
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
        } else if (this.additionalKeysFromView != null) {
          this.currItr = this.additionalKeysFromView.iterator();
          this.additionalKeysFromView = null;
        } else if (this.regionsIndex < this.numSubRegions) {
          // advance to next region
          createIterator(this.regions.get(this.regionsIndex));
          ++this.regionsIndex;
        } else {
          return null;
        }
      }
    }

    private void createIterator(final LocalRegion rgn) {
      // TX iterates over KEYS.
      // NonTX iterates over RegionEntry instances
      this.currRgn = rgn;
      this.currItr = view.getRegionKeysForIteration(rgn).iterator();
      this.additionalKeysFromView = view.getAdditionalKeysForIterator(rgn);
    }
  }

  @Override
  public int size() {
    checkTX();
    if (this.iterType == IteratorType.VALUES) {
      // if this is a values-view, then we have to filter out nulls to
      // determine the correct size
      int s = 0;
      for (Iterator<Object> itr = new EntriesIterator(); itr.hasNext(); itr.next()) {
        s++;
      }
      return s;
    } else if (this.recursive) {
      return this.topRegion.allEntriesSize();
    } else {
      return view.entryCount(this.topRegion);
    }
  }

  @Override
  public Object[] toArray() {
    return toArray(null);
  }

  @Override
  public Object[] toArray(final Object[] array) {
    checkTX();
    final ArrayList<Object> temp = new ArrayList<Object>(this.size());
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
    return this.keepSerialized;
  }

  public void setIgnoreCopyOnReadForQuery(boolean ignoreCopyOnReadForQuery) {
    this.ignoreCopyOnReadForQuery = ignoreCopyOnReadForQuery;
  }

  public boolean isIgnoreCopyOnReadForQuery() {
    return this.ignoreCopyOnReadForQuery;
  }

}
