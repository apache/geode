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
package org.apache.geode.internal.size;

import java.lang.ref.PhantomReference;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.classloader.ClassPathLoader;
import org.apache.geode.internal.size.ObjectTraverser.Visitor;
import org.apache.geode.util.internal.GeodeGlossary;


public class ObjectGraphSizer {
  private static final String SIZE_OF_CLASS_NAME =
      System.getProperty(GeodeGlossary.GEMFIRE_PREFIX + "ObjectSizer.SIZE_OF_CLASS",
          ReflectionSingleObjectSizer.class.getName());
  @Immutable
  static final SingleObjectSizer SIZE_OF_UTIL;
  @Immutable
  private static final ObjectFilter NULL_FILTER = new ObjectFilter() {
    @Override
    public boolean accept(Object parent, Object object) {
      return true;
    }

  };

  static {
    Class sizeOfClass;
    try {
      sizeOfClass = ClassPathLoader.getLatest().forName(SIZE_OF_CLASS_NAME);
      SIZE_OF_UTIL = new CachingSingleObjectSizer((SingleObjectSizer) sizeOfClass.newInstance());
    } catch (Exception e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * Find the size of an object and all objects reachable from it using breadth first search. This
   * is equivalent to calling
   *
   * <pre>
   * size(root, false);
   * </pre>
   *
   */
  // TODO
  // - native byte buffers
  // - native memory (how??)
  public static long size(Object root) throws IllegalArgumentException, IllegalAccessException {
    return size(root, false);
  }

  /**
   * Find the size of an object and all objects reachable from it using breadth first search. This
   * is equivalent to calling set(root, filter, includeStatics) where the filter will accept all
   * objects.
   *
   */
  public static long size(Object root, boolean includeStatics)
      throws IllegalArgumentException, IllegalAccessException {
    return size(root, NULL_FILTER, includeStatics);
  }

  /**
   * Find the size of an object and all objects reachable from it using breadth first search. This
   * method will include objects reachable from static fields. Using this method requires some heap
   * space - probably between 8 - 30 bytes per reachable object.
   *
   * Objects reachable only through weak or soft references will not be considered part of the total
   * size.
   *
   * @param root the object to size
   * @param filter that can exclude objects from being counted in the results. If an object is not
   *        accepted, it's size will not be included and it's children will not be visited unless
   *        they are reachable by some other path.
   * @param includeStatics if set to true, static members of a class will be traversed the first
   *        time that a class is encountered.
   *
   */
  public static long size(Object root, ObjectFilter filter, boolean includeStatics)
      throws IllegalArgumentException, IllegalAccessException {
    SizeVisitor visitor = new SizeVisitor(filter);
    ObjectTraverser.breadthFirstSearch(root, visitor, includeStatics);

    return visitor.getTotalSize();
  }

  public static String histogram(Object root, boolean includeStatics)
      throws IllegalArgumentException, IllegalAccessException {
    return histogram(root, NULL_FILTER, includeStatics);
  }

  public static String histogram(Object root, ObjectFilter filter, boolean includeStatics)
      throws IllegalArgumentException, IllegalAccessException {
    HistogramVistor visitor = new HistogramVistor(filter);
    ObjectTraverser.breadthFirstSearch(root, visitor, includeStatics);

    return visitor.dump();

  }

  private static class HistogramVistor implements ObjectTraverser.Visitor {
    private final Map<Class, Integer> countHisto = new HashMap<Class, Integer>();
    private final Map<Class, Long> sizeHisto = new HashMap<Class, Long>();
    private final ObjectFilter filter;

    public HistogramVistor(ObjectFilter filter) {
      this.filter = filter;
    }

    @Override
    public boolean visit(Object parent, Object object) {
      if (!filter.accept(parent, object)) {
        return false;
      }
      Integer count = countHisto.get(object.getClass());
      if (count == null) {
        count = Integer.valueOf(1);
      } else {
        count = Integer.valueOf(count.intValue() + 1);
      }

      countHisto.put(object.getClass(), count);

      long objectSize;
      try {
        objectSize = SIZE_OF_UTIL.sizeof(object);
      } catch (IllegalArgumentException e) {
        throw new RuntimeException(e);
      }
      Long size = sizeHisto.get(object.getClass());
      if (size == null) {
        size = Long.valueOf(objectSize);
      } else {
        size = Long.valueOf(size.longValue() + objectSize);
      }

      sizeHisto.put(object.getClass(), size);

      return true;
    }

    public String dump() {
      StringBuilder result = new StringBuilder();
      result.append("clazz\tsize\tcount\n");
      Set<HistogramEntry> orderedSize = getOrderedSet();
      for (HistogramEntry entry : orderedSize) {
        Class clazz = entry.clazz;
        Integer count = entry.count;
        Long size = entry.size;
        result.append(clazz + "\t" + size + "\t" + count + "\n");
      }
      return result.toString();
    }

    public Set<HistogramEntry> getOrderedSet() {
      TreeSet<HistogramEntry> result = new TreeSet<HistogramEntry>();
      for (Map.Entry<Class, Long> entry : sizeHisto.entrySet()) {
        Class clazz = entry.getKey();
        Long size = entry.getValue();
        Integer count = countHisto.get(clazz);
        result.add(new HistogramEntry(clazz, count, size));
      }
      return result;
    }

    private static class HistogramEntry implements Comparable<HistogramEntry> {
      private final Class clazz;
      private final Integer count;
      private final Long size;

      public HistogramEntry(Class clazz, Integer count, Long size) {
        this.size = size;
        this.clazz = clazz;
        this.count = count;
      }



      @Override
      public int compareTo(HistogramEntry o) {
        int diff = size.compareTo(o.size);
        if (diff == 0) {
          diff = clazz.getName().compareTo(o.clazz.getName());
        }
        return diff;
      }

      @Override
      public String toString() {
        return size.toString();
      }
    }
  }

  private static class SizeVisitor implements Visitor {
    private long totalSize;
    private final ObjectFilter filter;

    public SizeVisitor(ObjectFilter filter) {
      this.filter = filter;
    }

    @Override
    public boolean visit(Object parent, Object object) {
      if (!filter.accept(parent, object)) {
        return false;
      }

      totalSize += SIZE_OF_UTIL.sizeof(object);
      // We do want to include the size of the reference itself, but
      // we don't visit the children because they will be GC'd if there is no
      // other reference
      return !(object instanceof WeakReference) && !(object instanceof SoftReference)
          && !(object instanceof PhantomReference);
    }

    public long getTotalSize() {
      return totalSize;
    }
  }



  public interface ObjectFilter {
    boolean accept(Object parent, Object object);
  }

  private ObjectGraphSizer() {}

}
