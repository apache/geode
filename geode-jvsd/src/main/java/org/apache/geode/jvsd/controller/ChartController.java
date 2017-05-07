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
package org.apache.geode.jvsd.controller;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import javafx.beans.binding.Bindings;
import javafx.collections.FXCollections;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.geometry.Side;
import javafx.scene.chart.Axis;
import javafx.scene.chart.NumberAxis;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.layout.Pane;
import javafx.stage.Stage;

import org.apache.geode.jvsd.model.StatArchiveFile.StatValue;
import org.apache.geode.jvsd.util.ChartManager;
import org.apache.geode.jvsd.util.Utility;
import org.apache.geode.jvsd.view.javafx.scene.chart.AdvancedLineChart;
import org.apache.geode.jvsd.view.javafx.scene.chart.BasicSeries;
import org.apache.geode.jvsd.view.javafx.scene.chart.Data;
import org.apache.geode.jvsd.view.javafx.scene.chart.DateAxis;
import org.apache.geode.jvsd.view.javafx.scene.chart.LongDoubleMemoryMappedSeries;

/**
 * An advanced line chart with a variety of actions and settable properties.
 *
 * @see javafx.scene.chart.LineChart
 * @see javafx.scene.chart.Chart
 * @see javafx.scene.chart.NumberAxis
 * @see javafx.scene.chart.XYChart
 */
public class ChartController implements Initializable {

  private int chartId;

  private Stage stage;

  private AdvancedLineChart<Number, Number> chart;

  private ChartManager manager = ChartManager.INSTANCE;

  @FXML
  private Pane root;

  @FXML
  private Button resetButton;

  @FXML
  private Label coordLabel;

  public void setStage(Stage stage) {
    this.stage = stage;
    this.stage.setOnCloseRequest(eh -> {
      manager.removeChart(chartId);
    });
  }

  public void setId(int chartId) {
    this.chartId = chartId;
  }

  @Override
  public void initialize(URL url, ResourceBundle resourceBundle) {
    chart = createChart(root);
    root.getChildren().add(chart);

    // TODO better way to do this?
    coordLabel.textProperty().bind(Bindings.format("%s x %s",
        Bindings.createStringBinding(() -> DateFormat.getInstance().format(
                chart.getXAxis().getValueForDisplay(
                    chart.crosshairXProperty().doubleValue())),
            chart.crosshairXProperty()),
        Bindings.createStringBinding(() -> {
          final NumberFormat formatter = NumberFormat.getInstance();
          formatter.setGroupingUsed(true);
          return formatter.format(
              chart.getPrimaryYAxis().getValueForDisplay(
                  chart.crosshairYProperty().doubleValue()));
        }, chart.crosshairYProperty(), chart.primaryYAxisProperty())));
  }

  @FXML
  private void resetRangeClicked() {
    chart.getXAxis().setAutoRanging(true);
    for (Axis<Number> yAxis : chart.getYAxes()) {
      yAxis.setAutoRanging(true);
    }
  }

  protected AdvancedLineChart<Number, Number> createChart(Pane root) {
    final DateAxis xAxis = new DateAxis();
    xAxis.setAutoRanging(true);
    final NumberAxis yAxis = new NumberAxis();
    final AdvancedLineChart<Number, Number> lc =
        new AdvancedLineChart<>(xAxis, yAxis);
    lc.setCreateSymbols(false);
    lc.prefWidthProperty().bind(root.widthProperty());
    lc.prefHeightProperty().bind(root.heightProperty());

    if (ChartManager.MMTEST) {
//    
//    try (final RandomAccessFile memoryMappedFile = new RandomAccessFile("/tmp/test.vsd.stat1.vss", "rw")) {
//
//      final MappedByteBuffer in = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, memoryMappedFile.length());
////      in.limit(1000000);
//      final LongDoubleMemoryMappedSeries s = new LongDoubleMemoryMappedSeries(in);
//      lc.getData().add(s);
//
////      Timeline animation = new Timeline();
////      animation.getKeyFrames().add(new KeyFrame(Duration.seconds(1), actionEvent -> {
////          in.limit(in.limit()+16);
////          System.out.println(in.limit());
//////          lc.toFront();
//////          Data<Number, Number> last = s.getData().get(s.getData().size() - 1);
//////          s.getData().add(new Data<>(last.getXValue().longValue() + 1000, last.getYValue().doubleValue() + (new Random().nextDouble() * 1000) - 500));
////        
////      }));
////     animation.setCycleCount(Animation.INDEFINITE);
////     animation.play();
//      
//
//    } catch (FileNotFoundException e) {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//    } catch (IOException e) {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//    }
    
    }

//  lc.getData().add(new LongDoubleFileSeries(new File("/tmp/test.vsd.stat1.vss")));

    return lc;
  }

  public void addToChart(StatValue sv) {
    if (!stage.titleProperty().isBound()) {
      stage.setTitle(
          "Chart" + chartId + "_" + sv.getDescriptor().getName());
    }

    Axis<Number> yAxis;
    final String units = sv.getDescriptor().getUnits();
    yAxis = findOrCreateYAxis(units);

    final long[] timestamps = sv.getRawAbsoluteTimeStamps();
    final double[] values = sv.getRawSnapshots();

    long timeSampleAverage = Utility.computeTimeSampleAverage(timestamps);
    long samplePeriod = timeSampleAverage / 1000;

    final List<Data<Number, Number>> data; 
    switch (sv.getFilter()) {
    case StatValue.FILTER_NONE:
      data = doFilterNone(samplePeriod, timestamps, values);
      break;
    case StatValue.FILTER_PERSEC:
      data = doFilterPerSec(samplePeriod, timestamps, values);
      break;
    default:
      data = null;
    }

    // add series data
    if (ChartManager.MMTEST) {
      addToChartMM(sv, yAxis, data);
    } else {
      BasicSeries<Number, Number> series = new BasicSeries<>(FXCollections.observableList(data));
      series.setYAxis(yAxis);
      series.setName(sv.getDescriptor().getName());
      chart.getData().add(series);
    }
  }

  private void addToChartMM(StatValue sv, Axis<Number> yAxis, List<Data<Number, Number>> data) {
    try {
      final File tmp = File.createTempFile(sv.getDescriptor().getName(), ".vss");
      tmp.deleteOnExit();
      
      try (final RandomAccessFile memoryMappedFile = new RandomAccessFile(tmp, "rw")) {
        final MappedByteBuffer out = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, data.size() * 16);

        for (Data<Number, Number> d : data) {
          out.putLong(d.getXValue().longValue());
          out.putDouble(d.getYValue().doubleValue());
        }
        out.force();

        final LongDoubleMemoryMappedSeries s = new LongDoubleMemoryMappedSeries(out);
        s.setYAxis(yAxis);
        s.setName(sv.getDescriptor().getName());
        chart.getData().add(s);
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  protected List<Data<Number, Number>> doFilterNone(long samplePeriod, final long[] timestamps, final double[] values) {
    final ArrayList<Data<Number, Number>> list = new ArrayList<>(timestamps.length - 1);
    // skip first value to calc value/s.
    for (int i = 1; i < timestamps.length; i++) {
      final Data<Number, Number> data = new Data<Number, Number>(timestamps[i], values[i]);

//TODO speed up      data.setNode(new HoveredThresholdNode(numberFormat.format(values[i])));
      list.add(data);
    }
    
    return list;
  }

  protected List<Data<Number, Number>> doFilterPerSec(long samplePeriod, final long[] timestamps, final double[] values) {
    final ArrayList<Data<Number, Number>> list = new ArrayList<>(timestamps.length - 1);

    // skip first value to calc value/s.
    for (int i = 1; i < timestamps.length; i++) {
      final long timestamp = timestamps[i];
      final double psValue = Utility.computePerSecondValue(timestamp, timestamps[i - 1], values[i], values[i - 1], samplePeriod);
      final Data<Number, Number> data = new Data<Number, Number>(timestamp, psValue);

//TODO speed up      data.setNode(new HoveredThresholdNode(numberFormat.format(psValue)));
      list.add(data);
    }
    
    return list;
  }

  private Axis<Number> findOrCreateYAxis(final String units) {
    Axis<Number> yAxis = findYAxis(units);

    if (null == yAxis) {
      yAxis = new NumberAxis();
      yAxis.setSide(Side.RIGHT);
      chart.getYAxes().add(yAxis);
    }

    if (null == yAxis.getLabel()) {
      yAxis.setLabel(units);
    }

    return yAxis;
  }

  private Axis<Number> findYAxis(final String units) {
    for (Axis<Number> y : chart.getYAxes()) {
      if (null == y.getLabel() || y.getLabel().equals(units)) {
        return y;
      }
    }

    return null;
  }


  public String getTitle() {
    return stage.getTitle();
  }
}
