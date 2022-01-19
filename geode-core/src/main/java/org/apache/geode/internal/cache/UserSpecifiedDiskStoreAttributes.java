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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.geode.internal.Assert;

/**
 * UserSpecifiedDiskStoreAttributes provides an way to detect departures from default attribute
 * values. It may be used when collecting attributes from an XML parser or from attribute changes
 * made using the {@link org.apache.geode.cache.DiskStoreFactory}. Its initial usage was to validate
 * when a user set a value which should not be set (for DiskStore).
 *
 * @since GemFire prPersistSprint2
 *
 */
public class UserSpecifiedDiskStoreAttributes extends DiskStoreAttributes {
  private boolean hasAutoCompact = false;
  private boolean hasCompactionThreshold = false;
  private boolean hasAllowForceCompaction = false;
  private boolean hasMaxOplogSize = false;
  private boolean hasTimeInterval = false;
  private boolean hasWriteBufferSize = false;
  private boolean hasQueueSize = false;
  private boolean hasDiskDirs = false;
  private boolean hasDiskDirSizes = false;
  private boolean hasDiskUsageWarningPercentage = false;
  private boolean hasDiskUsageCriticalPercentage = false;
  private static final int HAS_COUNT = 11;

  public boolean hasAutoCompact() {
    return hasAutoCompact;
  }

  public boolean hasCompactionThreshold() {
    return hasCompactionThreshold;
  }

  public boolean hasAllowForceCompaction() {
    return hasAllowForceCompaction;
  }

  public boolean hasMaxOplogSize() {
    return hasMaxOplogSize;
  }

  public boolean hasTimeInterval() {
    return hasTimeInterval;
  }

  public boolean hasWriteBufferSize() {
    return hasWriteBufferSize;
  }

  public boolean hasQueueSize() {
    return hasQueueSize;
  }

  public boolean hasDiskDirs() {
    return hasDiskDirs;
  }

  public boolean hasDiskDirSizes() {
    return hasDiskDirSizes;
  }

  public boolean hasDiskUsageWarningPercentage() {
    return hasDiskUsageWarningPercentage;
  }

  public boolean hasDiskUsageCriticalPercentage() {
    return hasDiskUsageCriticalPercentage;
  }

  public void setHasAutoCompact(boolean hasAutoCompact) {
    this.hasAutoCompact = hasAutoCompact;
  }

  public void setHasCompactionThreshold(boolean hasCompactionThreshold) {
    this.hasCompactionThreshold = hasCompactionThreshold;
  }

  public void setHasAllowForceCompaction(boolean hasAllowForceCompaction) {
    this.hasAllowForceCompaction = hasAllowForceCompaction;
  }

  public void setHasMaxOplogSize(boolean hasMaxOplogSize) {
    this.hasMaxOplogSize = hasMaxOplogSize;
  }

  public void setHasTimeInterval(boolean hasTimeInterval) {
    this.hasTimeInterval = hasTimeInterval;
  }

  public void setHasWriteBufferSize(boolean hasWriteBufferSize) {
    this.hasWriteBufferSize = hasWriteBufferSize;
  }

  public void setHasQueueSize(boolean hasQueueSize) {
    this.hasQueueSize = hasQueueSize;
  }

  public void setHasDiskDirs(boolean hasDiskDirs) {
    this.hasDiskDirs = hasDiskDirs;
  }

  public void setHasDiskDirSizes(boolean hasDiskDirSizes) {
    this.hasDiskDirSizes = hasDiskDirSizes;
  }

  public void setHasDiskUsageWarningPercentage(boolean hasDiskUsageWarningPercentage) {
    this.hasDiskUsageWarningPercentage = true;
  }

  public void setHasDiskUsageCriticalPercentage(boolean hasDiskUsageCriticalPercentage) {
    this.hasDiskUsageCriticalPercentage = true;
  }

  public void setAllHasFields(boolean b) {
    int hasCounter = 0;
    Field[] thisFields = UserSpecifiedDiskStoreAttributes.class.getDeclaredFields();
    for (final Field thisField : thisFields) {
      if (thisField.getName().startsWith("has")) {
        hasCounter++;
        try {
          thisField.setBoolean(this, b);
        } catch (IllegalAccessException ouch) {
          Assert.assertTrue(false,
              "Could not access field" + thisField.getName() + " on " + getClass());
        }
      }
    }
    Assert.assertTrue(hasCounter == HAS_COUNT, "Found " + hasCounter + " methods");
  }

  public void initHasFields(UserSpecifiedDiskStoreAttributes other) {
    Field[] thisFields = UserSpecifiedDiskStoreAttributes.class.getDeclaredFields();
    Object[] emptyArgs = new Object[] {};
    int hasCounter = 0;
    String fieldName = null;
    for (final Field thisField : thisFields) {
      fieldName = thisField.getName();
      if (fieldName.startsWith("has")) {
        hasCounter++;
        boolean bval = false;

        try {
          Method otherMeth = other.getClass().getMethod(fieldName/* , (Class[])null */);
          bval = (Boolean) otherMeth.invoke(other, emptyArgs);
        } catch (NoSuchMethodException darnit) {
          Assert.assertTrue(false, "A has* method accessor is required for field " + fieldName);
        } catch (IllegalAccessException boom) {
          Assert.assertTrue(false, "Could not access method " + fieldName + " on " + getClass());
        } catch (IllegalArgumentException e) {
          Assert.assertTrue(false,
              "Illegal argument trying to set field " + e.getLocalizedMessage());
        } catch (InvocationTargetException e) {
          Assert.assertTrue(false, "Failed trying to invoke method " + e.getLocalizedMessage());
        }

        try {
          thisField.setBoolean(this, bval);
        } catch (IllegalAccessException ouch) {
          Assert.assertTrue(false, "Could not access field" + fieldName + " on " + getClass());
        }
      }
    }
    Assert.assertTrue(hasCounter == HAS_COUNT,
        "Expected " + HAS_COUNT + " methods, got " + hasCounter + " last field: " + fieldName);
    Assert.assertTrue(thisFields.length == HAS_COUNT);
  }
}
