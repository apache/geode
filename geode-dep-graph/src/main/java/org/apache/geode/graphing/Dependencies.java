/*
 * Licensed to the Apache Software Foundation (ASF) under one or
 * more contributor license
 * agreements. See the NOTICE file distributed with this work for
 * additional information regarding
 * copyright ownership. The ASF licenses this file to You under
 * the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with
 * the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under
 * the License.
 */
package org.apache.geode.graphing;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.graphstream.graph.Graph;
import org.graphstream.graph.IdAlreadyInUseException;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.MultiGraph;
import org.graphstream.ui.layout.Layout;
import org.graphstream.ui.layout.Layouts;
import org.graphstream.ui.spriteManager.Sprite;
import org.graphstream.ui.spriteManager.SpriteManager;
import org.graphstream.ui.swingViewer.GraphRenderer;
import org.graphstream.ui.swingViewer.ViewPanel;
import org.graphstream.ui.view.Viewer;

public class Dependencies {
  public static void main(String args[]) {

    Graph graph = new MultiGraph("Tutorial 1");
    SpriteManager spriteManager = new SpriteManager(graph);
    graph.addAttribute("ui.stylesheet", "node { fill-mode: dyn-plain;fill-color: green, red; }");

    String rootPath =
        "/Users/ukohlmeyer/projects/geode/geode-assembly/build/install/apache-geode/lib";

    String version = "1.14.0-build.0";

    File file = new File(rootPath);
    List<File> files =
        Arrays.stream(file.listFiles())
            .filter(file1 -> file1.getName().startsWith("geode-"))
            .filter(file1 -> !file1.getName().contains("geode-dependencies"))
            .filter(file1 -> !file1.getName().contains("gfsh-dependencies"))
            .filter(file1 -> !file1.getName().contains("geode-module-bootstrapping"))
            .collect(
                Collectors.toList());
    Map<String, List<String>> depsMap = new HashMap<>();

    files.forEach(file1 -> {
      try {
        JarFile jarFile = new JarFile(file1, true);
        Manifest manifest = jarFile.getManifest();
        String moduleName = manifest.getMainAttributes().getValue("Module-Name");
        try {
          String id = moduleName + "-" + version;
          Node node = graph.addNode(id);
          node.addAttribute("ui.label", id);
          node.addAttribute("ui.color", 0.1);
          Sprite sprite = spriteManager.addSprite(moduleName);
          sprite.attachToNode(id);
        } catch (IdAlreadyInUseException e) {
        }
        String depModules = manifest.getMainAttributes().getValue("Dependent-Modules");
        String depClassPath = manifest.getMainAttributes().getValue("Class-Path");

        ArrayList<String> arrayList = new ArrayList<>();
        arrayList.add(depClassPath);
        arrayList.add(depModules);
        depsMap.put(moduleName + "-" + version, arrayList);
        jarFile.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    });

    depsMap.forEach((name, deps) -> {
      String[] classPathDeps = new String[] {};
      // String[] classPathDeps =
      // !StringUtils.isEmpty(deps.get(0)) ? deps.get(0).split(" ") : new String[] {};
      String[] moduleDeps =
          !StringUtils.isEmpty(deps.get(1)) ? deps.get(1).split(" ") : new String[] {};

      Arrays.stream(moduleDeps)
          .forEach(moduleDep -> graph.addEdge(name + "-" + moduleDep, name, moduleDep, true));

      Arrays.stream(classPathDeps).forEach(classPathDep -> {
        try {
          String id = classPathDep;
          Node node = graph.addNode(id);
          node.addAttribute("ui.label", id);
          node.addAttribute("ui.color", 1);
          Sprite sprite = spriteManager.addSprite(id.replaceAll(".", "_"));
          sprite.attachToNode(id);
        } catch (IdAlreadyInUseException e) {
        }
        graph.addEdge(name + "-" + classPathDep, name, classPathDep, true);
      });

    });

    graph.addAttribute("ui.antialias");

    Viewer viewer = new Viewer(graph,
        Viewer.ThreadingModel.GRAPH_IN_ANOTHER_THREAD);
    GraphRenderer renderer = Viewer.newGraphRenderer();
    viewer.addView(Viewer.DEFAULT_VIEW_ID, renderer);
    Layout layout = Layouts.newLayoutAlgorithm();
    ViewPanel view = viewer.getDefaultView();
    view.setAutoscrolls(true);
    view.resizeFrame(1600, 1200);
    // view.getCamera().setViewPercent(0.8);
    viewer.enableAutoLayout(layout);
    // viewer.disableAutoLayout();
    viewer.enableXYZfeedback(true);

  }
}
