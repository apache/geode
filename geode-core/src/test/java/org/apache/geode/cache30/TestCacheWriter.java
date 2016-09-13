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
package org.apache.geode.cache30;

//import org.apache.geode.internal.LocalLogWriter;
import org.apache.geode.cache.*;

/**
 * A <code>CacheWriter</code> used in testing.  Its callback methods
 * are implemented to thrown {@link UnsupportedOperationException}
 * unless the user overrides the "2" methods.
 *
 * @see #wasInvoked
 *
 *
 * @since GemFire 3.0
 */
public abstract class TestCacheWriter extends TestCacheCallback
  implements CacheWriter {


  public final void beforeUpdate(EntryEvent event)
    throws CacheWriterException {

    this.invoked = true;
    beforeUpdate2(event);
  }

  public void beforeUpdate2(EntryEvent event)
    throws CacheWriterException {

    String s = "Unexpected callback invocation";
    throw new UnsupportedOperationException(s);
  }

  public final void beforeUpdate2(EntryEvent event, Object arg)
    throws CacheWriterException {

    String s = "Shouldn't be invoked";
    throw new UnsupportedOperationException(s);
  }

  public final void beforeCreate(EntryEvent event)
    throws CacheWriterException {

    this.invoked = true;
    beforeCreate2(event);
  }

  public void beforeCreate2(EntryEvent event)
    throws CacheWriterException {

    String s = "Unexpected callback invocation";
    throw new UnsupportedOperationException(s);
  }

  /**
   * Causes code that uses the old API to not compile
   */
  public final void beforeCreate2(EntryEvent event, Object arg)
    throws CacheWriterException {

    String s = "Shouldn't be invoked";
    throw new UnsupportedOperationException(s);
  }

  public final void beforeDestroy(EntryEvent event)
    throws CacheWriterException {

    this.invoked = true;
    beforeDestroy2(event);
  }

  public void beforeDestroy2(EntryEvent event)
    throws CacheWriterException {

    String s = "Unexpected callback invocation";
    throw new UnsupportedOperationException(s);
  }

  public final void beforeDestroy2(EntryEvent event, Object arg)
    throws CacheWriterException {

    String s = "Shouldn't be invoked";
    throw new UnsupportedOperationException(s);
  }

  public final void beforeRegionDestroy(RegionEvent event)
    throws CacheWriterException {

    // check argument to see if this is during tearDown
    if ("teardown".equals(event.getCallbackArgument())) return;

    this.invoked = true;
    beforeRegionDestroy2(event);
  }

  public void beforeRegionDestroy2(RegionEvent event)
    throws CacheWriterException {

    String s = "Unexpected callback invocation";
    throw new UnsupportedOperationException(s);
  }

  public final void beforeRegionDestroy2(RegionEvent event, Object arg)
    throws CacheWriterException {

    String s = "Shouldn't be invoked";
    throw new UnsupportedOperationException(s);
  }
  public final void beforeRegionClear(RegionEvent event)
    throws CacheWriterException {
    String s = "Unexpected callback invocation";
    throw new UnsupportedOperationException(s);
  }
}
