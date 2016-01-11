/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

var vMode = document.documentMode; // variable used for storing IE document
// mode

// start: flags used for checking whether the r-graph icon images already loaded
// or not
var hostNormalFlag = false;
var hostSevereFlag = false;
var hostErrorFlag = false;
var hostWarningFlag = false;
var memberLocatorManagerServerErrorFlag = false;
var memberLocatorServerErrorFlag = false;
var memberLocatorManagerErrorFlag = false;
var memberLocatorNormalFlag = false;
var memberLocatorSevereFlag = false;
var memberLocatorErrorFlag = false;
var memberServerSevereFlag = false;
var memberManagerSevereFlag = false;
var memberLocatorManagerSevereFlag = false;
var memberLocatorManagerServerSevereFlag = false;
var memberManagerServerSevereFlag = false;
var memberLocatorServerSevereFlag = false;
var memberServerErrorFlag = false;
var memberManagerServerErrorFlag = false;
var memberManagerErrorFlag = false;
var memberLocatorManagerServerWarningFlag = false;
var memberLocatorServerWarningFlag = false;
var memberLocatorManagerWarningFlag = false;
var memberLocatorWarningFlag = false;
var memberManagerServerWarningFlag = false;
var memberServerWarningFlag = false;
var memberManagerWarningFlag = false;
var memberLocatorManagerServerNormalFlag = false;
var memberLocatorServerNormalFlag = false;
var memberLocatorManagerNormalFlag = false;
var memberManagerServerNormalFlag = false;
var memberServerNormalFlag = false;
var memberManagerNormalFlag = false;
var memberNormalFlag = false;
var memberSevereFlag = false;
var memberErrorFlag = false;
var memberWarningFlag = false;
// end: flags used for checking whether the r-graph icon images already loaded
// or not

// function used for refreshing R Graph nodes color based on Alerts
function refreshNodeAccAlerts() {
  clusteRGraph.graph.eachNode(function(node) {

    if (node._depth == 2) {
      node.setData('cursor', 'pointer');
    }
    if (node._depth == 1) {
      var expandedNode = false;
      for ( var i = 0; i < expanededNodIds.length; i++) {
        if (expanededNodIds[i] == node.id) {
          expandedNode = true;
          break;
        }
      }

      if (!expandedNode) {

        clusteRGraph.op.contract(node, {
          type : 'animate',
          duration : 1,
          transition : $jit.Trans.linear
        });

        node.eachSubnode(function(childNode) {
          childNode.setData('type', 'circle');
          childNode.setData('dim', 1);
        });
      } else {
        clusteRGraph.op.expand(node, {
          type : 'animate',
          duration : 1,
          transition : $jit.Trans.linear
        });

        node.eachSubnode(function(childNode) {
          var edge = clusteRGraph.graph.getAdjacence(childNode.id, node.id);
          edge.setData('color', '#081628');
          var data = childNode.data;
          childNode.setData('type', data.nodeType);
          childNode.setData('dim', 14);
          var style = childNode.getData('style');
          style.cursor = 'pointer';
          style.fontSize = "2em";
          style.width = 25 + 'px';
          style.height = 27 + 'px';
          childNode.setData('cursor', 'pointer');
        });
      }
    }
  });
}

// function used for defining custom nodes for R-Graph
function customNodesDefination() {

  $jit.RGraph.Plot.NodeTypes.implement({
    'hostNormalNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 1) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!hostNormalFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
              hostNormalFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/normal.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 1) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });

  $jit.RGraph.Plot.NodeTypes.implement({
    'hostSevereNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 1) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!hostSevereFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
              hostSevereFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/severe.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 1) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });

  $jit.RGraph.Plot.NodeTypes.implement({
    'hostErrorNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 1) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!hostErrorFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
              hostErrorFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/error.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 1) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });

  $jit.RGraph.Plot.NodeTypes.implement({
    'hostWarningNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 1) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!hostWarningFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
              hostWarningFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/warning.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 1) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });

  $jit.RGraph.Plot.NodeTypes.implement({
    'memberLocatorNormalNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberLocatorNormalFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberLocatorNormalFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/normal-locators.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });

  $jit.RGraph.Plot.NodeTypes.implement({
    'memberLocatorErrorNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberLocatorErrorFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberLocatorErrorFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/error-locators.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });

  $jit.RGraph.Plot.NodeTypes.implement({
    'memberLocatorSevereNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberLocatorSevereFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberLocatorSevereFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/severe-locators.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });

  $jit.RGraph.Plot.NodeTypes.implement({
    'memberLocatorManagerServerSevereNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberLocatorManagerServerSevereFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberLocatorManagerServerSevereFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/severe-manager-locator-others.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });

  $jit.RGraph.Plot.NodeTypes.implement({
    'memberLocatorServerSevereNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberLocatorServerSevereFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberLocatorServerSevereFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/severe-locators-others.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });
  $jit.RGraph.Plot.NodeTypes.implement({
    'memberLocatorManagerSevereNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberLocatorManagerSevereFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberLocatorManagerSevereFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/severe-manager-locator.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });

  $jit.RGraph.Plot.NodeTypes.implement({
    'memberManagerSevereNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberManagerSevereFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberManagerSevereFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/severe-managers.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });
  $jit.RGraph.Plot.NodeTypes.implement({
    'memberManagerServerSevereNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberManagerServerSevereFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberManagerServerSevereFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/severe-managers-others.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });

  $jit.RGraph.Plot.NodeTypes.implement({
    'memberServerSevereNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberServerSevereFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberServerSevereFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/severe-others.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });
  $jit.RGraph.Plot.NodeTypes.implement({
    'memberLocatorManagerServerErrorNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberLocatorManagerServerErrorFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberLocatorManagerServerErrorFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/error-manager-locator-others.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });
  $jit.RGraph.Plot.NodeTypes.implement({
    'memberLocatorServerErrorNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberLocatorServerErrorFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberLocatorServerErrorFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/error-locators-others.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });
  $jit.RGraph.Plot.NodeTypes.implement({
    'memberLocatorManagerErrorNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberLocatorManagerErrorFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberLocatorManagerErrorFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/error-manager-locator.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });
  $jit.RGraph.Plot.NodeTypes.implement({
    'memberManagerServerErrorNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberManagerServerErrorFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberManagerServerErrorFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/error-managers-others.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });
  $jit.RGraph.Plot.NodeTypes.implement({
    'memberServerErrorNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberServerErrorFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberServerErrorFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/error-others.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });
  $jit.RGraph.Plot.NodeTypes.implement({
    'memberManagerErrorNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberManagerErrorFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberManagerErrorFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/error-managers.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });
  $jit.RGraph.Plot.NodeTypes.implement({
    'memberLocatorManagerServerWarningNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberLocatorManagerServerWarningFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberLocatorManagerServerWarningFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/warning-manager-locator-others.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });
  $jit.RGraph.Plot.NodeTypes.implement({
    'memberLocatorServerWarningNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberLocatorServerWarningFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberLocatorServerWarningFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/warning-locators-others.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });
  $jit.RGraph.Plot.NodeTypes.implement({
    'memberLocatorManagerWarningNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberLocatorManagerWarningFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberLocatorManagerWarningFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/warning-manager-locator.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });
  $jit.RGraph.Plot.NodeTypes.implement({
    'memberLocatorWarningNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberLocatorWarningFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberLocatorWarningFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/warning-locators.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });
  $jit.RGraph.Plot.NodeTypes.implement({
    'memberManagerServerWarningNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberManagerServerWarningFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberManagerServerWarningFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/warning-managers-others.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });
  $jit.RGraph.Plot.NodeTypes.implement({
    'memberServerWarningNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberServerWarningFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberServerWarningFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/warning-others.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });
  $jit.RGraph.Plot.NodeTypes.implement({
    'memberManagerWarningNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberManagerWarningFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberManagerWarningFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/warning-managers.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });
  $jit.RGraph.Plot.NodeTypes.implement({
    'memberLocatorManagerServerNormalNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberLocatorManagerServerNormalFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberLocatorManagerServerNormalFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/normal-manager-locator-others.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });
  $jit.RGraph.Plot.NodeTypes.implement({
    'memberLocatorServerNormalNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberLocatorServerNormalFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberLocatorServerNormalFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/normal-locators-others.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });
  $jit.RGraph.Plot.NodeTypes.implement({
    'memberLocatorManagerNormalNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberLocatorManagerNormalFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberLocatorManagerNormalFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/normal-manager-locator.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });
  $jit.RGraph.Plot.NodeTypes.implement({
    'memberManagerServerNormalNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberManagerServerNormalFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberManagerServerNormalFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/normal-managers-others.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });
  $jit.RGraph.Plot.NodeTypes.implement({
    'memberServerNormalNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberServerNormalFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberServerNormalFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/normal-others.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });
  $jit.RGraph.Plot.NodeTypes.implement({
    'memberManagerNormalNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberManagerNormalFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberManagerNormalFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/normal-managers.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });
  $jit.RGraph.Plot.NodeTypes.implement({
    'memberNormalNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberNormalFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberNormalFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/normal-otheruser.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });
  $jit.RGraph.Plot.NodeTypes.implement({
    'memberSevereNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberSevereFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberSevereFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/severe-otheruser.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });
  $jit.RGraph.Plot.NodeTypes.implement({
    'memberErrorNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberErrorFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberErrorFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/error-otheruser.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });
  $jit.RGraph.Plot.NodeTypes.implement({
    'memberWarningNode' : {
      'render' : function(node, canvas) {
        if (node._depth == 2) {
          var ctx = canvas.getCtx();
          var img = new Image();
          var pos = node.pos.getc(true);

          if (!memberWarningFlag) {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 13, pos.y - 13);
              memberWarningFlag = true;
            };
          } else {
            img.onload = function() {
              ctx.drawImage(img, pos.x - 10, pos.y - 10);
            };
          }
          img.src = 'images/warning-otheruser.png';
        }
      },
      'contains' : function(node, pos) {
        if (node._depth == 2) {
          var npos = node.pos.getc(true), dim = node.getData('dim');
          return this.nodeHelper.square.contains(npos, pos, dim);
        }
      }
    }
  });
}
// function used for creating empty Cluster Member R Graph
function createClusteRGraph() {
  customNodesDefination();
  var nodeParameters = {};
  var edgeParameters = {};

  if (vMode == 8) // check for IE 8 document mode
  {
    nodeParameters = {
      color : '#ddeeff',
      cursor : 'default'
    };

    edgeParameters = {
      color : '#081628',
      lineWidth : 1.5
    };
  } else {
    nodeParameters = {
      overridable : true,
      collapsed : true,
      type : 'hostNormalNode',
      dim : 12,
      cursor : 'pointer'
    };
    edgeParameters = {
      overridable : 'true',
      color : '#132634',
      lineWidth : 1.5
    };
  }

  clusteRGraph = new $jit.RGraph({
    injectInto : 'infovis',

    background : {
      CanvasStyles : {
        strokeStyle : '#081628'
      }
    },
    Navigation : {
      enable : false,
      panning : false
    },
    label : {
      type : 'Native',
      size : 10
    },
    Node : nodeParameters,

    Tips : {
      enable : true,
      offsetX : 5,
      offsetY : 5,
      type : 'Native',

      onShow : function(tip, node, isLeaf, domElement) {
        var html = "";
        var data = node.data;

        if (node._depth == 1) {

          html = "<div class=\"tip-title\"><div><div class='popupHeading'>"
              + node.name + "</div>" + "<div class='popupFirstRow'>"
              + "<div class='popupRowBorder borderBottomZero'>"
              + "<div class='labeltext left display-block width-70'>"
              + "<span class='left'>CPU Usage</span>"
              + "</div><div class='right width-30'>"
              + "<div class='color-d2d5d7 font-size14'>" + data.cpuUsage
              + "<span class='fontSize15'>%</span></span>"
              + "</div></div></div>"
              + "<div class='popupRowBorder borderBottomZero'>"
              + "<div class='labeltext left display-block width-70'>"
              + "<span class='left'>Memory Usage</span>"
              + "</div><div class='right width-30'>"
              + "<div class='color-d2d5d7 font-size14'>" + data.memoryUsage
              + "<span class='font-size15 paddingL5'>MB</span>"
              + "</div></div></div>"
              + "<div class='popupRowBorder borderBottomZero'>"
              + "<div class='labeltext left display-block width-70'>"
              + "<span class='left'>Load Avg.</span>"
              + "</div><div class='right width-30'>"
              + "<div class='color-d2d5d7 font-size14'>" + applyNotApplicableCheck(data.loadAvg)
              + "</div></div></div>"
              /*+ "<div class='popupRowBorder borderBottomZero'>"
              + "<div class='labeltext left display-block width-70'>"
              + "<span class='left'>Threads</span>"
              + "</div><div class='right width-30'>"
              + "<div class='color-d2d5d7 font-size14'>"
              + data.threads
              + "</div></div></div>"*/
              + "<div class='popupRowBorder borderBottomZero'>"
              + "<div class='labeltext left display-block width-70'>"
              + "<span class='left'>Sockets</span>"
              + "</div><div class='right width-30'>"
              + "<div class='color-d2d5d7 font-size14'>" + applyNotApplicableCheck(data.sockets)
              + "</div></div></div>" + "</div></div></div>";

        } else if (node._depth == 2) {
          var clients = 0;
          if (data.clients)
            clients = data.clients;

          html = "<div class=\"tip-title\"><div><div class='popupHeading'>"
              + node.name + "</div>" + "<div class='popupFirstRow'>"
              + "<div class='popupRowBorder borderBottomZero'>"
              + "<div class='labeltext left display-block width-70'>"
              + "<span class='left'>CPU Usage</span>"
              + "</div><div class='right width-30'>"
              + "<div class='color-d2d5d7 font-size14'>" + data.cpuUsage
              + "<span class='fontSize14'>%</span></span>"
              + "</div></div></div>"
              + "<div class='popupRowBorder borderBottomZero'>"
              + "<div class='labeltext left display-block width-70'>"
              + "<span class='left'>Threads</span>"
              + "</div><div class='right width-30'>"
              + "<div class='color-d2d5d7 font-size14'>" + data.numThreads
              + "<span class='fontSize14'></span></span>"
              + "</div></div></div>"
              + "<div class='popupRowBorder borderBottomZero'>"
              + "<div class='labeltext left display-block width-70'>"
              + "<span class='left'>JVM Pauses</span>"
              + "</div><div class='right width-30'>"
              + "<div class='color-d2d5d7 font-size14'>" + data.gcPauses
              + "</div></div></div>"
              + "<div class='popupRowBorder borderBottomZero'>"
              + "<div class='labeltext left display-block width-70'>"
              + "<span class='left'>"
              + jQuery.i18n.prop('pulse-regiontabletooltip-custom') + "</span>"
              + "</div><div class='right width-30'>"
              + "<div class='color-d2d5d7 font-size14'>" + data.regions
              + "</div></div></div>"
              + "<div class='popupRowBorder borderBottomZero'>"
              + "<div class='labeltext left display-block width-70'>"
              + "<span class='left'>Clients</span>"
              + "</div><div class='right width-30'>"
              + "<div class='color-d2d5d7 font-size14'>" + clients
              + "</div></div></div>"
              + "<div class='popupRowBorder borderBottomZero'>"
              + "<div class='labeltext left display-block width-70'>"
              + "<span class='left'>Gateway Sender</span>"
              + "</div><div class='right width-30'>"
              + "<div class='color-d2d5d7 font-size14'>" + data.gatewaySender
              + "</div></div></div>"
              + "<div class='popupRowBorder borderBottomZero'>"
              + "<div class='labeltext left display-block width-70'>"
              + "<span class='left'>Port</span>"
              + "</div><div class='right width-30'>"
              + "<div class='color-d2d5d7 font-size14'>" + data.port
              + "</div></div></div>"
              + "<div class='popupRowBorder borderBottomZero'>"
              + "<div class='labeltext left display-block width-70'>"
              + "<span class='left'>GemFire Version</span>"
              + "</div><div class='right width-30'>"
              + "<div class='color-d2d5d7 font-size14'>" + data.gemfireVersion
              + "</div></div></div>" + "</div></div></div>";
        }
        tip.innerHTML = html;
      }
    },
    Edge : edgeParameters,
    Events : {
      enable : true,
      type : 'Native',

      onClick : function(node, eventInfo, e) {
        if (!node)
          return;

        if (node.nodeFrom) {
          // it's an edge
        } else {
          if (node._depth == 2)
            location.href = 'MemberDetails.html?member=' + node.id
                + '&memberName=' + node.name;
        }
      }
    },
    onCreateLabel : function(domElement, node) {
      domElement.innerHTML = node.name;
      domElement.onclick = function() {
        clusteRGraph.onClick(node.id, {
          onComplete : function() {
            if (vMode != 8) {
              if (node._depth == 1) {
                var nodeExist = false;
                for ( var i = 0; i < expanededNodIds.length; i++) {
                  if (expanededNodIds[i] == node.id) {
                    expanededNodIds.splice(i, 1);
                    clusteRGraph.op.contract(node, {
                      type : 'animate',
                      duration : 1,
                      transition : $jit.Trans.linear
                    });
                    node.eachSubnode(function(childNode) {
                      childNode.setData('type', 'circle');
                      childNode.setData('dim', 1);
                    });
                    nodeExist = true;
                    break;
                  }
                }
                if (!nodeExist) {

                  expanededNodIds[expanededNodIds.length] = node.id;
                  clusteRGraph.op.expand(node, {
                    type : 'animate',
                    duration : 1,
                    transition : $jit.Trans.linear
                  });
                  node.eachSubnode(function(childNode) {
                    var edge = clusteRGraph.graph.getAdjacence(childNode.id,
                        node.id);
                    edge.setData('color', '#081628');
                    var data = childNode.data;
                    childNode.setData('type', data.nodeType);
                    childNode.setData('dim', 14);
                  });
                }
              } else {
                location.href = 'MemberDetails.html?member=' + node.id
                    + '&memberName=' + node.name;
              }
            } else {
              if (node._depth == 2) {
                location.href = 'MemberDetails.html?member=' + node.id
                    + '&memberName=' + node.name;
              }
            }
          }
        });
      };
    },

    onPlaceLabel : function(domElement, node) {
      if (vMode != 8) {
        var style = domElement.style;
        style.cursor = 'pointer';
        if (node._depth == 2) {
          style.fontSize = "2em";
          style.width = 25 + 'px';
          style.height = 27 + 'px';
          style.opacity = "0";
          style.filter = "alpha(opacity=0)"; // for IE need to check
          var left = parseInt(style.left);
          var w = domElement.offsetWidth;
          style.left = (left - w / 2) + 5 + 'px';
          var top = parseInt(style.top);
          var t = top - 12;
          style.top = t + 'px';
        }

        if (node._depth == 1) {
          style.fontSize = "2em";
          style.width = 25 + 'px';
          style.height = 27 + 'px';
          style.opacity = "0";
          style.filter = "alpha(opacity=0)"; // for IE need to check
          var pars = node.getParents();
          if (clusteRGraph.graph.getAdjacence(node.id, pars[0].id) != null) {
            var edge = clusteRGraph.graph.getAdjacence(node.id, pars[0].id);
            edge.setData('color', '#132634');
          }
          var left = parseInt(style.left);
          var w = domElement.offsetWidth;
          style.left = (left - w / 2) + 'px';
          var top = parseInt(style.top);
          var t = top - 10;
          style.top = t + 'px';
        }

        if (node._depth == 0) {
          style.display = 'none';
          node.setData('dim', 0);
        }
      } else {
        var style = domElement.style;
        style.display = '';
        style.cursor = 'default';

        if (node._depth == 1) {
          style.fontSize = "0.8em";
          style.color = "#ccc";

        } else if (node._depth == 2) {
          style.fontSize = "0.7em";
          style.color = "#494949";

        } else {
          style.display = 'none';
        }

        var left = parseInt(style.left);
        var w = domElement.offsetWidth;
        style.left = (left - w / 2) + 'px';
      }

    }
  });
}
// updating rGraph custom node flag to default false value
function updateRGraphFlags() {

  hostNormalFlag = false;
  hostSevereFlag = false;
  hostErrorFlag = false;
  hostWarningFlag = false;
  memberLocatorManagerServerErrorFlag = false;
  memberLocatorServerErrorFlag = false;
  memberLocatorManagerErrorFlag = false;
  memberLocatorNormalFlag = false;
  memberLocatorSevereFlag = false;
  memberLocatorErrorFlag = false;
  memberServerSevereFlag = false;
  memberManagerSevereFlag = false;
  memberLocatorManagerSevereFlag = false;
  memberLocatorManagerServerSevereFlag = false;
  memberManagerServerSevereFlag = false;
  memberLocatorServerSevereFlag = false;
  memberServerErrorFlag = false;
  memberManagerServerErrorFlag = false;
  memberManagerErrorFlag = false;
  memberLocatorManagerServerWarningFlag = false;
  memberLocatorServerWarningFlag = false;
  memberLocatorManagerWarningFlag = false;
  memberLocatorWarningFlag = false;
  memberManagerServerWarningFlag = false;
  memberServerWarningFlag = false;
  memberManagerWarningFlag = false;
  memberLocatorManagerServerNormalFlag = false;
  memberLocatorServerNormalFlag = false;
  memberLocatorManagerNormalFlag = false;
  memberManagerServerNormalFlag = false;
  memberServerNormalFlag = false;
  memberManagerNormalFlag = false;
  memberNormalFlag = false;
  memberSevereFlag = false;
  memberErrorFlag = false;
  memberWarningFlag = false;
  eval(functionStartArray[4]);
}
