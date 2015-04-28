package com.gemstone.gemfire.internal.cache.persistence.query;

public class IdentityExtractor implements SortKeyExtractor {

  @Override
  public Object getSortKey(Object element) {
    return element;
  }

}
