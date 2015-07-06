package com.pivotal.jvsd.fx;

import com.pivotal.jvsd.controller.RootController;
import com.pivotal.jvsd.model.stats.StatArchiveFile;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.scene.layout.Pane;
import javafx.stage.Stage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Basic container for all charts which are currently being displayed
 *
 * @author Jens Deppe
 */
public class ChartManager {

  static final boolean MMTEST = true;

  private Map<Integer, VSDChartWindow> charts = new HashMap<>();

  private static ChartManager instance = new ChartManager();

  private RootController root;

  private int chartId = 0;

  private ChartManager() {
    // We are a singleton
  }

  public static ChartManager getInstance() {
    return instance;
  }

  public VSDChartWindow add(VSDChartWindow chart) {
    return chart;
  }

  public VSDChartWindow get(String chartName) {
    return charts.get(chartName);
  }

  public void setRootController(RootController root) {
    this.root = root;
  }

  public void removeChart(int id) {
    charts.remove(id);
    root.setChartList(charts.keySet());
  }

  public int newChart(StatArchiveFile.StatValue sv) throws IOException {
    int id = chartId++;

    FXMLLoader loader = new FXMLLoader(getClass().getResource("/chart.fxml"));
    Pane chartPane = loader.load();
    Stage stage = new Stage();
    Scene scene = new Scene(chartPane);
    scene.getStylesheets().add("META-INF/css/style.css");
    stage.setScene(scene);
    stage.show();

    VSDChartWindow controller = loader.getController();
    controller.setId(id);
    charts.put(id, controller);
    root.setChartList(charts.keySet());

    controller.setStage(stage);
//    if (!MMTEST) {
      controller.addToChart(sv);
//    }

    return id;
  }

  public void addToChart(int id, StatArchiveFile.StatValue sv) throws IOException {
    charts.get(id).addToChart(sv);
  }
}
