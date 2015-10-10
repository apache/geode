package com.pivotal.jvsd.controller;

import com.pivotal.jvsd.fx.ChartManager;
import com.pivotal.jvsd.fx.Main;
import com.pivotal.jvsd.fx.StatFileManager;
import com.pivotal.jvsd.model.ResourceWrapper;
import com.pivotal.jvsd.model.stats.StatArchiveFile;
import com.pivotal.jvsd.model.stats.StatArchiveFile.StatValue;
import java.io.File;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.ListView;
import javafx.scene.control.SelectionMode;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.cell.PropertyValueFactory;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;
import javafx.event.ActionEvent;
import javafx.scene.control.MenuItem;
import javafx.stage.FileChooser;

/**
 * @author Jens Deppe
 */
public class RootController implements Initializable {

  @FXML
  private TableView<ResourceWrapper> tableView;

  @FXML
  private ListView listView;

  @FXML
  private Button newChartButton;

  @FXML
  private Button addToChartButton;

  @FXML
  private ComboBox chartCombo;

  private ObservableList<Integer> chartList;

  private ObservableList<ResourceWrapper> resources;

  private ObservableList statNames;

  private StatFileManager fileManager = StatFileManager.getInstance();

  private ChartManager chartManager = ChartManager.getInstance();

  @Override
  public void initialize(URL url, ResourceBundle resourceBundle) {
    double[] columnPercent = {
        0.05,  // Row
        0.25,  // Start Time
        0.05,  // File
        0.075, // Samples
        0.075, // PID
        0.2,   // Type
        0.3    // Name
    };

    ObservableList<TableColumn<ResourceWrapper, ?>> cols = tableView.getColumns();
    // Set column widths
    for (int i = 0; i < 7; i++) {
      cols.get(i).prefWidthProperty().
          bind(tableView.widthProperty().multiply(columnPercent[i]));
    }

    cols.get(0).setCellValueFactory(new PropertyValueFactory<>("row"));
    cols.get(1).setCellValueFactory(new PropertyValueFactory<>("startTime"));
    cols.get(2).setCellValueFactory(new PropertyValueFactory<>("file"));
    cols.get(3).setCellValueFactory(new PropertyValueFactory<>("samples"));
    cols.get(4).setCellValueFactory(new PropertyValueFactory<>("pid"));
    cols.get(5).setCellValueFactory(new PropertyValueFactory<>("type"));
    cols.get(6).setCellValueFactory(new PropertyValueFactory<>("name"));

    resources = FXCollections.observableArrayList();

        updateArchives();

    tableView.setItems(resources);
    tableView.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);
    tableView.getSelectionModel().selectedItemProperty().addListener(
        (observableValue, oldValue, newValue) -> {
          updateStatList();
        });

    statNames = FXCollections.observableArrayList();
    listView.setItems(statNames);
    listView.getSelectionModel().setSelectionMode(SelectionMode.SINGLE);
    listView.getSelectionModel().selectedItemProperty().addListener(
        (observableValue, oldValue, newValue) -> {
          System.out.println(newValue);
        });

    chartList = FXCollections.observableArrayList();
    chartCombo.setItems(chartList);
  }

    private void updateArchives() {
        int row = 0;
        for (StatArchiveFile s : fileManager.getArchives()) {
            for (StatArchiveFile.ResourceInst r : s.getResourceInstList()) {
                resources.add(new ResourceWrapper(r, row++));
            }
        }
    }

  private List<StatValue> getSelectedStatValues() {
    List<StatValue> result = new ArrayList<>();

    List<String> statNames = listView.getSelectionModel().getSelectedItems();
    ObservableList<ResourceWrapper> rows =
        tableView.getSelectionModel().getSelectedItems();
    if (rows != null && rows.size() > 0) {
      for (ResourceWrapper r : rows) {
        for (String stat : statNames) {
          StatValue sv = r.getStatValue(stat);
          result.add(sv);
        }
      }
    }

    return result;
  }

  @FXML
  private void newChartButtonClicked() {
    List<StatValue> svs = getSelectedStatValues();
    if (svs.size() > 0) {
      try {
        int id = chartManager.newChart(svs.get(0));
        svs.remove(0);
        for (StatValue s : svs) {
          chartManager.addToChart(id, s);
        }
        chartCombo.getSelectionModel().select(id);
      } catch (IOException iox) {
        iox.printStackTrace();
      }
    }
  }

  @FXML
  private void addToChartButtonClicked() {
    Integer i = (Integer) chartCombo.getSelectionModel().getSelectedItem();
    List<StatValue> svs = getSelectedStatValues();
    try {
      for (StatValue s : svs) {
        chartManager.addToChart(i, s);
      }
    } catch (IOException iox) {
      iox.printStackTrace();
    }
  }

  @FXML
  private void handleOpenMenuAction(ActionEvent e){
      try {
          FileChooser fileChooser = new FileChooser();
          fileChooser.setTitle("Open Statistics File");
          File file=fileChooser.showOpenDialog(null);
          StatFileManager.getInstance().add(file.getCanonicalPath());
          updateArchives();
      } catch (IOException ex) {
          //TODO needs warning dialog of failed file open.
      }
  }
  
  @FXML
  private void handleCloseMenuAction(ActionEvent e){
    //TODO do a real clean up and shutdown   
    //just kill it now
      System.exit(0);
  }
  
  public void setChartList(Collection<Integer> chartList) {
    if (chartList.size() > 0) {
      chartCombo.setDisable(false);
      this.chartList.clear();
      this.chartList.addAll(chartList);
      this.chartCombo.getSelectionModel().select(0);
      addToChartButton.setDisable(false);
    } else {
      this.chartList.clear();
      chartCombo.setDisable(true);
      addToChartButton.setDisable(true);
    }

  }

  private void updateStatList() {
    ObservableList<ResourceWrapper> rows =
        tableView.getSelectionModel().getSelectedItems();
    List<String> tmpStatNames = rows.get(0).getStatNames();

    for (int i = 1; i < rows.size(); i++) {
      List<String> tmpNames = rows.get(i).getStatNames();
      Iterator<String> it = tmpStatNames.iterator();
      while (it.hasNext()) {
        String s = it.next();
        if (!tmpNames.contains(s)) {
          it.remove();
        }
      }
    }

    statNames.clear();
    statNames.addAll(tmpStatNames);
  }
}
