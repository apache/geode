package com.pivotal.javafx.scene.chart;

import java.util.List;

public interface DataSet<X, Y> extends javafx.beans.Observable {
  public List<Data<X, Y>> getData(X from, X to, int limit);

  public void addListener(DataChangedListener listener);

  public void removeListener(DataChangedListener listener);
}
