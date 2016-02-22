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

package com.gemstone.gemfire.internal.cache.extension;

import java.util.ArrayList;

import com.gemstone.gemfire.internal.util.CollectionUtils;

/**
 * Simple implementation of {@link ExtensionPoint} for easy integration with
 * existing objects.
 * 
 * @author jbarrett@pivotal.io
 *
 * @since 8.1
 */
// UnitTest SimpleExtensionPointJUnitTest
public class SimpleExtensionPoint<T> implements ExtensionPoint<T> {

  protected final ArrayList<Extension<T>> extensions = new ArrayList<Extension<T>>();

  protected final Iterable<Extension<T>> iterable = CollectionUtils.unmodifiableIterable(extensions);

  protected final Extensible<T> extensible;

  protected final T target;

  /**
   * Construct a new {@link SimpleExtensionPoint} around the given extensible
   * target.
   * 
   * @param extensible
   *          the {@link Extensible} object this extension point acts on.
   * 
   * @param target
   *          the <code>T</code> instance being extended. Likely the same as
   *          <code>exensible</code>.
   * @since 8.1
   */
  public SimpleExtensionPoint(final Extensible<T> extensible, final T target) {
    this.extensible = extensible;
    this.target = target;
  }

  @Override
  public Iterable<Extension<T>> getExtensions() {
    return iterable;
  }

  @Override
  public void addExtension(Extension<T> extension) {
    extensions.add(extension);
  }
  
  @Override
  public void removeExtension(Extension<T> extension) {
    extensions.remove(extension);
  }

  @Override
  public T getTarget() {
    return target;
  }

  public void fireCreate(final Extensible<T> newTarget) {
    for (final Extension<T> extension : extensions) {
      extension.onCreate(extensible, newTarget);
    }
  }
}
