package com.gemstone.gemfire.cache.query.internal;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenCustomHashMap;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.cache.query.types.ObjectType;

@Deprecated
public class OrderByComparatorUnmapped extends OrderByComparator {

  private final Map<Object, Object[]> orderByMap;

  public OrderByComparatorUnmapped(List<CompiledSortCriterion> orderByAttrs,
      ObjectType objType, ExecutionContext context) {
    super(orderByAttrs, objType, context);
    if (objType.isStructType()) {
      orderByMap = new Object2ObjectOpenCustomHashMap<Object, Object[]>(
          new StructBag.ObjectArrayFUHashingStrategy());
    } else {
      this.orderByMap = new HashMap<Object, Object[]>();
    }

  }

  @Override
  void addEvaluatedSortCriteria(Object row, ExecutionContext context)
      throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {

    this.orderByMap.put(row, this.calculateSortCriteria(context, row));
  }

  @Override
  protected Object[] evaluateSortCriteria(Object row) {
    return (Object[]) orderByMap.get(row);
  }


  private Object[] calculateSortCriteria(ExecutionContext context, Object row)

  throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {

    CompiledSortCriterion csc;
    if (orderByAttrs != null) {
      Object[] evaluatedResult = new Object[this.orderByAttrs.size()];

      Iterator<CompiledSortCriterion> orderiter = orderByAttrs.iterator();
      int index = 0;
      while (orderiter.hasNext()) {
        csc = orderiter.next();
        Object[] arr = new Object[2];
        if (csc.getColumnIndex() == -1) {
          arr[0] = csc.evaluate(context);
        } else {
          arr[0] = csc.evaluate(row, context);
        }
        arr[1] = Boolean.valueOf(csc.getCriterion());
        evaluatedResult[index++] = arr;
      }
      return evaluatedResult;
    }
    return null;
  }

}
