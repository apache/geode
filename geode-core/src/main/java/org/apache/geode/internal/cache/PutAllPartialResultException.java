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

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;

import org.apache.geode.CancelException;
import org.apache.geode.GemFireException;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;

/**
 * This exception is thrown if some sub-ops failed during a bulk operation. Current bulk ops are
 * putAll and removeAll. Note: the name of this class is not changed to BulkOpPartialResultException
 * to keep it compatible with old clients and old peers.
 *
 *
 *
 * @since GemFire 6.5
 */
public class PutAllPartialResultException extends GemFireException {

  private static final long serialVersionUID = 2411654400733621071L;

  private final PutAllPartialResult result;

  //////////////////// Constructors ////////////////////

  public PutAllPartialResultException(PutAllPartialResult result) {
    super("Bulk operation had some failures");
    this.result = result;
  }

  public PutAllPartialResultException() {
    super("Bulk operation had some failures");
    result = new PutAllPartialResult(-1);
  }

  /**
   * consolidate exceptions
   */
  public void consolidate(PutAllPartialResultException pre) {
    result.consolidate(pre.getResult());
  }

  public void consolidate(PutAllPartialResult otherResult) {
    result.consolidate(otherResult);
  }

  public void setSucceededKeysAndVersions(VersionedObjectList keysAndVersions) {
    result.setSucceededKeysAndVersions(keysAndVersions);
  }

  public PutAllPartialResult getResult() {
    return result;
  }

  /**
   * Returns the key set in exception
   */
  public VersionedObjectList getSucceededKeysAndVersions() {
    return result.getSucceededKeysAndVersions();
  }

  public Exception getFailure() {
    return result.getFailure();
  }

  /**
   * Returns there's failedKeys
   */
  public boolean hasFailure() {
    return result.hasFailure();
  }

  public Object getFirstFailedKey() {
    return result.getFirstFailedKey();
  }

  @Override
  public String getMessage() {
    return result.toString();
  }

  public static class PutAllPartialResult implements Serializable {
    private static final long serialVersionUID = -2168767259323206954L;

    private VersionedObjectList succeededKeys;
    private Object firstFailedKey;
    private Exception firstCauseOfFailure;
    private int totalMapSize;

    //////////////////// Constructors ////////////////////

    public PutAllPartialResult(int totalMapSize) {
      succeededKeys = new VersionedObjectList();
      this.totalMapSize = totalMapSize;
    }

    public void setTotalMapSize(int totalMapSize) {
      this.totalMapSize = totalMapSize;
    }

    public void setSucceededKeysAndVersions(VersionedObjectList other) {
      synchronized (this) {
        succeededKeys = other;
      }
    }

    public void consolidate(PutAllPartialResult other) {
      synchronized (this) {
        succeededKeys.addAll(other.getSucceededKeysAndVersions());
      }
      saveFailedKey(other.firstFailedKey, other.firstCauseOfFailure);
    }

    public Exception getFailure() {
      return firstCauseOfFailure;
    }

    /**
     * record all succeeded keys in a version list response
     */
    public void addKeysAndVersions(VersionedObjectList keysAndVersions) {
      synchronized (this) {
        succeededKeys.addAll(keysAndVersions);
      }
    }

    /**
     * record all succeeded keys when there are no version results
     */
    public void addKeys(Collection<?> keys) {
      synchronized (this) {
        if (succeededKeys.getVersionTags().size() > 0) {
          throw new IllegalStateException(
              "attempt to store versionless keys when there are already versioned results");
        }
        succeededKeys.addAllKeys(keys);
      }
    }


    /**
     * increment failed key number
     */
    public void saveFailedKey(Object key, Exception cause) {
      if (key == null) {
        return;
      }
      // we give high priority for CancelException
      if (firstFailedKey == null || cause instanceof CancelException) {
        firstFailedKey = key;
        firstCauseOfFailure = cause;
      }
    }

    /**
     * Returns the key set in exception
     */
    public VersionedObjectList getSucceededKeysAndVersions() {
      return succeededKeys;
    }

    /**
     * Returns the first key that failed
     */
    public Object getFirstFailedKey() {
      return firstFailedKey;
    }

    /**
     * Returns there's failedKeys
     */
    public boolean hasFailure() {
      return firstFailedKey != null;
    }

    /**
     * Returns there's saved succeed keys
     */
    public boolean hasSucceededKeys() {
      return succeededKeys.size() > 0;
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer("Key " + firstFailedKey
          + " and possibly others failed the operation due to " + firstCauseOfFailure + "\n");
      if (totalMapSize > 0) {
        int failedKeyNum = totalMapSize - succeededKeys.size();
        sb.append("The bulk operation failed on " + failedKeyNum + " out of " + totalMapSize
            + " entries. ");
      }
      return sb.toString();
    }

    public String detailString() {
      StringBuffer sb = new StringBuffer(toString());
      sb.append(getKeyListString());
      return sb.toString();
    }

    public String getKeyListString() {
      StringBuffer sb = new StringBuffer();

      sb.append("The keys for the successful entries are: ");
      int cnt = 0;
      final Iterator iterator = succeededKeys.iterator();
      while (iterator.hasNext()) {
        Object key = iterator.next();
        sb.append(" ").append(key);
        cnt++;
      }
      return sb.toString();
    }
  }
}
