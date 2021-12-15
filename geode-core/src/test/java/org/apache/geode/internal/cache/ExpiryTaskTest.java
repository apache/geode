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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.ExpirationAttributes;


public class ExpiryTaskTest {

  private static class TestableExpiryTask extends ExpiryTask {

    public TestableExpiryTask(LocalRegion localRegion) {
      super(localRegion);
    }

    @Override
    protected ExpirationAttributes getIdleAttributes() {
      return null;
    }

    @Override
    protected ExpirationAttributes getTTLAttributes() {
      return null;
    }

    @Override
    protected void basicPerformTimeout(boolean isPending) throws CacheException {}

    @Override
    protected void reschedule() throws CacheException {}

    @Override
    protected long getLastModifiedTime() throws EntryNotFoundException {
      return 0;
    }

    @Override
    protected long getLastAccessedTime() throws EntryNotFoundException {
      return 0;
    }

    @Override
    protected boolean invalidate() throws CacheException {
      return false;
    }

    @Override
    protected boolean destroy(boolean isPending) throws CacheException {
      return false;
    }

    @Override
    protected boolean localInvalidate() throws EntryNotFoundException {
      return false;
    }

    @Override
    protected boolean localDestroy() throws CacheException {
      return false;
    }

    @Override
    protected void addExpiryTask() throws EntryNotFoundException {}

    @Override
    public boolean isPending() {
      return false;
    }

    @Override
    public Object getKey() {
      return null;
    }
  }

  @Test
  public void shouldBeMockable() throws Exception {
    ExpiryTask mockExpiryTask = mock(ExpiryTask.class);
    mockExpiryTask.run2();
    verify(mockExpiryTask, times(1)).run2();
  }

  @Test
  public void isExpiryUnitsSecondsReturnsTrueIfNoRegion() {
    ExpiryTask expiryTask = new TestableExpiryTask(null);

    assertThat(expiryTask.isExpiryUnitSeconds()).isTrue();
  }

  @Test
  public void isExpiryUnitsSecondsReturnsTrueIfRegionNotMilliseconds() {
    LocalRegion localRegion = mock(LocalRegion.class);
    when(localRegion.isExpiryUnitsMilliseconds()).thenReturn(false);
    ExpiryTask expiryTask = new TestableExpiryTask(localRegion);

    assertThat(expiryTask.isExpiryUnitSeconds()).isTrue();
  }

  @Test
  public void isExpiryUnitsSecondsReturnsFalseIfRegionIsMilliseconds() {
    LocalRegion localRegion = mock(LocalRegion.class);
    when(localRegion.isExpiryUnitsMilliseconds()).thenReturn(true);
    ExpiryTask expiryTask = new TestableExpiryTask(localRegion);

    assertThat(expiryTask.isExpiryUnitSeconds()).isFalse();
  }

  @Test
  public void normalizeToMillisConvertIfUnitsAreSeconds() {
    LocalRegion localRegion = mock(LocalRegion.class);
    when(localRegion.isExpiryUnitsMilliseconds()).thenReturn(false);
    ExpiryTask expiryTask = new TestableExpiryTask(localRegion);

    long result = expiryTask.normalizeToMillis(2);

    assertThat(result).isEqualTo(2000);
  }

  @Test
  public void normalizeToMillisDoesNothingIfExpiryUnitMillis() {
    LocalRegion localRegion = mock(LocalRegion.class);
    when(localRegion.isExpiryUnitsMilliseconds()).thenReturn(true);
    ExpiryTask expiryTask = new TestableExpiryTask(localRegion);

    long result = expiryTask.normalizeToMillis(2000);

    assertThat(result).isEqualTo(2000);
  }

  @Test
  public void hasExpiredReturnsFalseIfNowBeforeExpireTime() {
    ExpiryTask expiryTask = new TestableExpiryTask(null);

    boolean result = expiryTask.hasExpired(1499, 2000);

    assertThat(result).isFalse();
  }

  @Test
  public void hasExpiredReturnsTrueIfNowEqualsExpireTime() {
    ExpiryTask expiryTask = new TestableExpiryTask(null);

    boolean result = expiryTask.hasExpired(2000, 2000);

    assertThat(result).isTrue();
  }

  @Test
  public void hasExpiredReturnsTrueIfNowAfterExpireTime() {
    ExpiryTask expiryTask = new TestableExpiryTask(null);

    boolean result = expiryTask.hasExpired(3000, 2000);

    assertThat(result).isTrue();
  }

  @Test
  public void hasExpiredReturnsTrueIfNowLessThanHalfSecondOfExpireTime() {
    ExpiryTask expiryTask = new TestableExpiryTask(null);

    boolean result = expiryTask.hasExpired(1500, 2000);

    assertThat(result).isTrue();
  }
}
