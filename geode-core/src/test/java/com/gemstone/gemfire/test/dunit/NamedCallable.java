package com.gemstone.gemfire.test.dunit;


public class NamedCallable<T> implements SerializableCallableIF<T> {

  private static final long serialVersionUID = -4417299628656632541L;

  String name;
  SerializableCallableIF<T> delegate;
  
  public NamedCallable(String name, SerializableCallableIF<T> delegate) {
    this.name = name;
    this.delegate = delegate;
  }
  
  @Override
  public T call() throws Exception {
    return delegate.call();
  }
  
  @Override
  public String toString() {
    return ("callable("+name+")");
  }

}
