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
package org.apache.geode.sequence;

import org.apache.geode.internal.sequencelog.GraphType;
import org.apache.geode.internal.sequencelog.io.Filter;
import org.apache.geode.internal.sequencelog.io.GraphReader;
import org.apache.geode.internal.sequencelog.model.*;
import org.apache.geode.sequence.*;

import javax.swing.*;
import java.awt.event.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

/**
 */
public class GemfireSequenceDisplay {

  private JLabel selectedGraphsLabel;
  private SelectGraphDialog selectGraphDialog;

  private Map<GraphID, Map<String, Lifeline>> lineMap = new HashMap();
  private Map<GraphID, List<Arrow>> arrowMap = new HashMap();
  private SequenceDiagram sequenceDiagram;
  private JFrame frame;
  private SequencePanel sequencePanel;

  /**
   * Create the GUI and show it.  For thread safety,
   * this method should be invoked from the
   * event-dispatching thread.
   *
   * @param graphs
   * @param lineMapper 
   */
  private void createAndShowGUI(final GraphSet graphs, LineMapper lineMapper) {
    //Create and set up the window.

    frame = new JFrame("SequenceDiagram");
    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

    createMenu();
    createSequenceDiagram(graphs, lineMapper);
    createSequenceMaps(graphs);

    createSelectGraphDialog(graphs);

    //        for (GraphID id : graphs.getMap().keySet()) {
    //            showSubDiagram(id);
    //
    //        }

    sequencePanel = new SequencePanel(sequenceDiagram);
    frame.getContentPane().add(sequencePanel);
    //Display the window.
    frame.pack();
    frame.setVisible(true);
    showGraphSelector();
  }

  private void createMenu() {
    JMenuBar menuBar = new JMenuBar();

    JMenu sequenceMenu = new JMenu("Sequence");
    sequenceMenu.setMnemonic(KeyEvent.VK_S);
    sequenceMenu.getAccessibleContext().setAccessibleDescription(
        "The only menu in this program that has menu items");
    menuBar.add(sequenceMenu);
    JMenuItem selectGraphs = new JMenuItem("Choose Graphs",
        KeyEvent.VK_G);
    selectGraphs.setAccelerator(KeyStroke.getKeyStroke(
        KeyEvent.VK_G, ActionEvent.ALT_MASK));
    selectGraphs.getAccessibleContext().setAccessibleDescription(
        "Select what graphs to display");
    selectGraphs.setActionCommand("selectgraphs");
    selectGraphs.addActionListener(new ActionListener() {

      public void actionPerformed(ActionEvent e) {
        showGraphSelector();
      }
    });

    sequenceMenu.add(selectGraphs);
    frame.setJMenuBar(menuBar);
  }

  private void createSelectGraphDialog(final GraphSet graphs) {
    selectGraphDialog = new SelectGraphDialog(graphs);
    selectGraphDialog.addSelectionListener(new SelectGraphDialog.SelectionListener() {

      public void selectionChanged(List<GraphID> selectedIds) {
        updateGraphs(selectedIds);
      }
    });
    selectGraphDialog.pack();
  }

  private void updateGraphs(List<GraphID> selectedIds) {
    List<GraphID> existingDiagrams =(List) sequenceDiagram.getSubDiagramsNames();
    for(GraphID id : selectedIds) {
      showSubDiagram(id);
      existingDiagrams.remove(id);
    }
    for(GraphID id : existingDiagrams) {
      hideSubDiagram(id);
    }

    sequenceDiagram.resizeMe(sequenceDiagram.getWidth(), sequenceDiagram.getHeight());
    sequencePanel.revalidate();
    sequencePanel.repaint();
    //        sequenceDiagram.revalidate();
    //        sequenceDiagram.repaint();
  }

  private void showGraphSelector() {
    selectGraphDialog.setVisible(true);
  }

  private void hideGraphSelector() {
    selectGraphDialog.setVisible(false);
  }

  //  private static SequenceDiagram createSequenceDiagram() {
  //      long startTime = System.currentTimeMillis();
  //      List<Lifeline> lines = new ArrayList<Lifeline>();
  //      List<Arrow> arrows = new ArrayList<Arrow>();
  //      for(int i =0 ; i < 10; i++) {
  //          List<LifelineState> states = new ArrayList<LifelineState>();
  //          for(int j =0; j < 5; j++) {
  //              LifelineState state = new LifelineState(startTime  + 20* j, startTime  + 20 * j + 20);
  //              states.add(state);
  //          }
  //          Lifeline line = new Lifeline(i, states);
  //          lines.add(line);
  //
  //          if(i > 0) {
  //              Arrow arrow = new Arrow("arrow" + i, line, lines.get(i - 1), line.getStates().get(2));
  //              arrows.add(arrow);
  //          }
  //      }
  //
  //      SequenceDiagram diag = new SequenceDiagram(startTime, startTime + 20 * 5, lines, arrows);
  //      return diag;
  //  }

  private void createSequenceMaps(GraphSet graphs) {

    Map<GraphID, Graph> map = graphs.getMap();
    for (Map.Entry<GraphID, Graph> entry : map.entrySet()) {
      GraphID graphId = entry.getKey();
      Graph graph = entry.getValue();
      Map<String, Lifeline> lines = new LinkedHashMap<String, Lifeline>(graphs.getLocations().size());
      List<Arrow> arrows = new ArrayList<Arrow>();
      Map<Vertex, LifelineState> states = new HashMap<Vertex, LifelineState>();
      for (String location : graphs.getLocations()) {
        lines.put(location, new Lifeline(graphId, location));
      }

      Collection<Edge> edges = graph.getEdges();
      for (Edge edge : edges) {
        Vertex dest = edge.getDest();
        Vertex source = edge.getSource();
        if (dest == null) {
          dest = source;
        }
        if (source == null) {
          source = dest;
        }
        LifelineState destState = states.get(dest);
        if (destState == null) {
          final Lifeline lifeline = lines.get(dest.getName());
          destState = createState(lifeline, graphs, dest);
          lifeline.addState(destState);
          states.put(dest, destState);
        }
        LifelineState sourceState = states.get(source);
        if (sourceState == null) {
          final Lifeline lifeline = lines.get(source.getName());
          sourceState = createState(lifeline, graphs, source);
          lifeline.addState(sourceState);
          states.put(source, sourceState);
        }
        Arrow arrow = new Arrow(edge.getName(), sourceState, destState);
        arrows.add(arrow);
        destState.addInboundArrow(arrow);
      }


      lineMap.put(graphId, lines);
      arrowMap.put(graphId, arrows);
    }
  }

  public void showSubDiagram(GraphID id) {
    sequenceDiagram.addSubDiagram(id, lineMap.get(id), arrowMap.get(id));
  }

  public void hideSubDiagram(GraphID id) {
    sequenceDiagram.removeSubDiagram(id);
  }

  private SequenceDiagram createSequenceDiagram(GraphSet graphs, LineMapper lineMapper) {

    sequenceDiagram = new SequenceDiagram(graphs.getMinTime(), graphs.getMaxTime(), graphs.getLocations(), lineMapper);
    return sequenceDiagram;
  }

  private static LifelineState createState(Lifeline lifeline, GraphSet graphs, Vertex dest) {
    long start = dest.getTimestamp();
    long end = dest.getNextVertexOnDest() == null ? graphs.getMaxTime() : dest.getNextVertexOnDest().getTimestamp();
    return new LifelineState(lifeline, dest.getState(), start, end);
  }

  public static void main(String[] args) throws IOException {
    File[] files;
    Set<String> keyFilters = new HashSet<String>();
    boolean areGemfireLogs = false;
    if (args.length > 0) {
      ArrayList<File> fileList = new ArrayList<File>();
      for (int i =0; i < args.length; i++) {
        String arg = args[i];
        if(arg.equals("-filterkey")) {
          keyFilters.add(args[i+1]);
          i++;
        } else if(arg.equals("-logs")) {
          areGemfireLogs = true;
        }
        else {
          fileList.add(new File(args[i]));
        }
        
      }
      files = fileList.toArray(new File[0]);
    } else {
      System.err.println("Usage: java -jar sequence.jar (-logs) (-filterkey key)* <file>+\n\n" +
                "\t-logs (expiremental) instead of using .graph files, parse the gemfire logs to generate the sequence display" +
      		"\t-filterkey a java regular expression to match against key names. If specified\n" +
      		"The list of key sequence diagrams will only contain matching keys");
      System.exit(1);
      return;
    }
    
    final GraphSet graphs;
    
    graphs = getGraphs(areGemfireLogs, keyFilters, files);

    final LineMapper lineMapper = getLineMapper(files);
    final GemfireSequenceDisplay display = new GemfireSequenceDisplay();
    //Schedule a job for the event-dispatching thread:
    //creating and showing this application's GUI.
    javax.swing.SwingUtilities.invokeLater(new Runnable() {
      public void run() {
        display.createAndShowGUI(graphs, lineMapper);
      }
    });
  }

  private static GraphSet getGraphs(boolean useLogFiles, Set<String> keyFilters, File[] files)
      throws IOException {
    Filter graphFilter = new KeyFilter(keyFilters);
    
    
    GraphReader reader = new GraphReader(files);
    final GraphSet graphs;
    if(keyFilters.isEmpty()) {
      graphs = reader.readGraphs(useLogFiles);
    } else {
      graphs = reader.readGraphs(graphFilter, useLogFiles);
    }
    return graphs;
  }

  /**
   * @param files 
   * @return
   */
  private static LineMapper getLineMapper(File[] files) {
    if(HydraLineMapper.isInHydraRun(files)) {
      return new HydraLineMapper(files);
    } else {
      return new DefaultLineMapper();
    }
  }
  
  private static class KeyFilter implements Filter {
    Set<Pattern> patterns = new HashSet<Pattern>();
    

    public KeyFilter(Set<String> keyFilters) {
      for(String filterString : keyFilters) {
        patterns.add(Pattern.compile(filterString));
      }
    }

    public boolean accept(GraphType graphType, String name, String edgeName,
        String source, String dest) {
      if(graphType.equals(GraphType.KEY)) {
        for(Pattern pattern : patterns) {
          if(pattern.matcher(name).find()) {
            return true;
          }
        }
        
        return false;
      } else {
        return true;
      }
    }

    public boolean acceptPattern(GraphType graphType, Pattern pattern,
        String edgeName, String source, String dest) {
      return true;
    }
    
  }

}
