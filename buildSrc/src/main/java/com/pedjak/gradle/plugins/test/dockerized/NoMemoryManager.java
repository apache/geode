/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pedjak.gradle.plugins.test.dockerized;

import org.gradle.process.internal.health.memory.JvmMemoryStatusListener;
import org.gradle.process.internal.health.memory.MemoryHolder;
import org.gradle.process.internal.health.memory.MemoryManager;
import org.gradle.process.internal.health.memory.OsMemoryStatusListener;

/**
 * A no-op implementation of Memory Manager
 * TODO: For what purpose?
 */
public class NoMemoryManager implements MemoryManager {
  @Override
  public void addListener(JvmMemoryStatusListener jvmMemoryStatusListener) {

  }

  @Override
  public void addListener(OsMemoryStatusListener osMemoryStatusListener) {

  }

  @Override
  public void removeListener(JvmMemoryStatusListener jvmMemoryStatusListener) {

  }

  @Override
  public void removeListener(OsMemoryStatusListener osMemoryStatusListener) {

  }

  @Override
  public void addMemoryHolder(MemoryHolder memoryHolder) {

  }

  @Override
  public void removeMemoryHolder(MemoryHolder memoryHolder) {

  }

  @Override
  public void requestFreeMemory(long l) {

  }
}
