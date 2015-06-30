/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.soplog.nofile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplog.SortedOplogWriter;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplogFactory.SortedOplogConfiguration;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReader;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReaderTestCase;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class NoFileSortedOplogJUnitTest extends SortedReaderTestCase {
  private NoFileSortedOplog mfile;

  @Override
  protected SortedReader<ByteBuffer> createReader(NavigableMap<byte[], byte[]> data) throws IOException {
    mfile = new NoFileSortedOplog(new SortedOplogConfiguration("nofile"));

    SortedOplogWriter wtr = mfile.createWriter();
    for (Entry<byte[], byte[]> entry : data.entrySet()) {
      wtr.append(entry.getKey(), entry.getValue());
    }
    wtr.close(null);

    return mfile.createReader();
  }
}
