package com.gemstone.gemfire.internal.cache.persistence.query;

public interface SortKeyExtractor {
  Object getSortKey(Object element);
}
