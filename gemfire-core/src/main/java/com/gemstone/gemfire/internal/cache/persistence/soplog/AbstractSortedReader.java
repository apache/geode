/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * Provides default behavior for range scans.
 *  
 * @author bakera
 */
public abstract class AbstractSortedReader implements SortedReader<ByteBuffer> {
  @Override
  public final SortedIterator<ByteBuffer> scan() throws IOException {
    return scan(null, true, null, true);
  }

  @Override
  public final SortedIterator<ByteBuffer> head(byte[] to, boolean inclusive)  throws IOException{
    return scan(null, true, to, inclusive);
  }

  @Override
  public final SortedIterator<ByteBuffer> tail(byte[] from, boolean inclusive)  throws IOException{
    return scan(from, inclusive, null, true);
  }

  @Override
  public final SortedIterator<ByteBuffer> scan(byte[] from, byte[] to)  throws IOException{
    return scan(from, true, to, false);
  }

  @Override
  public final SortedIterator<ByteBuffer> scan(byte[] equalTo)  throws IOException{
    return scan(equalTo, true, equalTo, true);
  }

  @Override
  public SortedIterator<ByteBuffer> scan(byte[] from, boolean fromInclusive, byte[] to,
      boolean toInclusive)  throws IOException{
    return scan(from, fromInclusive, to, toInclusive, true, null);
  }

  @Override
  public final SortedReader<ByteBuffer> withAscending(boolean ascending) {
    if (this instanceof DelegateSortedReader) {
      DelegateSortedReader tmp = (DelegateSortedReader) this;
      return new DelegateSortedReader(tmp.delegate, ascending, tmp.filter);
    }
    return new DelegateSortedReader(this, ascending, null);
  }

  @Override
  public final SortedReader<ByteBuffer> withFilter(MetadataFilter filter) {
    if (this instanceof DelegateSortedReader) {
      DelegateSortedReader tmp = (DelegateSortedReader) this;
      return new DelegateSortedReader(tmp.delegate, tmp.ascending, filter);
    }
    return new DelegateSortedReader(this, true, filter);
  }
  
  protected class DelegateSortedReader extends AbstractSortedReader {
    /** the embedded reader */
    private final AbstractSortedReader delegate;
    
    /** true if ascending */
    private final boolean ascending;
    
    /** the filter */
    private final MetadataFilter filter;
    
    public DelegateSortedReader(AbstractSortedReader reader, boolean ascending, MetadataFilter filter) {
      this.delegate = reader;
      this.ascending = ascending;
      this.filter = filter;
    }
    
    @Override
    public boolean mightContain(byte[] key) throws IOException {
      return delegate.mightContain(key);
    }

    @Override
    public ByteBuffer read(byte[] key) throws IOException {
      return delegate.read(key);
    }

    @Override
    public SerializedComparator getComparator() {
      return delegate.getComparator();
    }

    @Override
    public SortedStatistics getStatistics() throws IOException {
      return delegate.getStatistics();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }

    @Override
    public SortedIterator<ByteBuffer> scan(
        byte[] from, boolean fromInclusive, 
        byte[] to, boolean toInclusive) throws IOException {
      return scan(from, fromInclusive, to, toInclusive, ascending, filter);
    }

    @Override
    public SortedIterator<ByteBuffer> scan(
        byte[] from, boolean fromInclusive,
        byte[] to, boolean toInclusive, 
        boolean ascending, 
        MetadataFilter filter) throws IOException {
      return delegate.scan(from, fromInclusive, to, toInclusive, ascending, filter);
    }
  }
}
