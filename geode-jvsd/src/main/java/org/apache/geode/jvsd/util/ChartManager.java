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
package org.apache.geode.jvsd.util;

import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.scene.layout.Pane;
import javafx.stage.Stage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.geode.jvsd.controller.RootController;
import org.apache.geode.jvsd.controller.ChartController;
import org.apache.geode.jvsd.model.StatArchiveFile;

/**
 * Basic container for all charts which are currently being displayed
 *
 * @author Jens Deppe
 */
public enum ChartManager {
	INSTANCE;

	private RootController root;
	public static final boolean MMTEST = true;
	private AtomicInteger chartId = new AtomicInteger(0);
	private Map<Integer, ChartController> charts = new HashMap<>();

	/**
	 * @param chartName
	 * @return
	 */
	public ChartController get(String chartName) {
		return charts.get(chartName);
	}

	/**
	 * @param root
	 */
	public void setRootController(RootController root) {
		this.root = root;
	}

	/**
	 * @param id
	 */
	public void removeChart(int id) {
		charts.remove(id);
		root.setChartList(charts.keySet());
	}

	/**
	 * @param sv
	 * @return
	 * @throws IOException
	 */
	public int newChart(StatArchiveFile.StatValue sv) throws IOException {
		int id = chartId.incrementAndGet();

		FXMLLoader loader = new FXMLLoader(getClass().getResource("/views/chart.fxml"));
		Pane chartPane = loader.load();
		Stage stage = new Stage();
		Scene scene = new Scene(chartPane);
		scene.getStylesheets().add("styles/style.css");
		stage.setScene(scene);
		stage.show();

		ChartController controller = loader.getController();
		controller.setId(id);
		charts.put(id, controller);
		root.setChartList(charts.keySet());

		controller.setStage(stage);
		controller.addToChart(sv);

		return id;
	}

	/**
	 * @param id
	 * @param sv
	 * @throws IOException
	 */
	public void addToChart(int id, StatArchiveFile.StatValue sv) throws IOException {
		charts.get(id).addToChart(sv);
	}
}
