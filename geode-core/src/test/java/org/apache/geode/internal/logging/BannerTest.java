/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.logging;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.services.module.impl.ServiceLoaderModuleService;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Unit tests for {@link org.apache.geode.internal.logging.Banner} and {@link
 * org.apache.geode.internal.logging.Banner.BannerHeader}.
 */
@Category(LoggingTest.class)
public class BannerTest {

  private String banner;

  @Before
  public void setUp() {
    banner = new Banner(new ServiceLoaderModuleService(LogService.getLogger())).getString();
  }

  @Test
  public void moreThanZeroBannerHeaderValues() {
    assertThat(Banner.BannerHeader.values().length).isGreaterThan(0);
  }

  @Test
  public void moreThanZeroBannerHeaderDisplayValues() {
    assertThat(Banner.BannerHeader.displayValues().length).isGreaterThan(0);
  }

  @Test
  public void displayValuesReturnsDisplayValueForEveryBannerHeader() {
    for (Banner.BannerHeader bannerHeader : Banner.BannerHeader.values()) {
      assertThat(Banner.BannerHeader.displayValues()).contains(bannerHeader.displayValue());
    }
  }

  @Test
  public void bannerContainsEveryBannerHeader() {
    assertThat(banner).contains(Banner.BannerHeader.displayValues());
  }
}
