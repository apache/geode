package org.apache.geode.internal.cache.tier.sockets;

import org.apache.geode.internal.cache.InterestEvent;
import org.apache.geode.internal.cache.InterestFilter;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * TestFilter is used by FilterProfileJUnitTest
 */
public class TestFilter implements InterestFilter {
  public TestFilter() {
  }

  @Override
  public void close() {
  }

  @Override
  public boolean notifyOnCreate(InterestEvent event) {
    return true;
  }

  @Override
  public boolean notifyOnUpdate(InterestEvent event) {
    return true;
  }

  @Override
  public boolean notifyOnDestroy(InterestEvent event) {
    return false;
  }

  @Override
  public boolean notifyOnInvalidate(InterestEvent event) {
    return false;
  }

  @Override
  public boolean notifyOnRegister(InterestEvent event) {
    return false;
  }
}
