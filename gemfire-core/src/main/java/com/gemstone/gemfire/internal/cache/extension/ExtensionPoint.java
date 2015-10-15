/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.extension;

/**
 * Allows {@link Extensible} objects to add and remove {@link Extension}s.
 * 
 * @author jbarrett@pivotal.io
 *
 * @since 8.1
 */
public interface ExtensionPoint<T> {

  /**
   * Add {@link Extension} to {@link ExtensionPoint}.
   * 
   * @param extension
   *          to add.
   * @since 8.1
   */
  void addExtension(Extension<T> extension);

  /**
   * Remove {@link Extension} from {@link ExtensionPoint}.
   * 
   * @param extension
   *          to remove.
   * @since 8.1
   */
  void removeExtension(Extension<T> extension);

  /**
   * Get {@link Iterable} of {@link Extension}s.
   * 
   * @return {@link Exception}s
   * @since 8.1
   */
  Iterable<Extension<T>> getExtensions();

  /**
   * Helper method to get appropriately typed access to target
   * {@link Extensible} object.
   * 
   * @return {@link Extensible} object target.
   * 
   * @since 8.1
   */
  T getTarget();

}
