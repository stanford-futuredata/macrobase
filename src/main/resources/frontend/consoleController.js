
var myApp = angular.module('consoleApp', ['ngStorage']);

myApp.service('explorerService', function($localStorage) {
    function setItems(ni) { localStorage.itemsJson = angular.toJson(ni); }
    function getItems() {   if('itemsJson' in localStorage) return localStorage.itemsJson }

    function markHiddenColumn(c) {
    if(!('hiddenColumns' in $localStorage)) {
        $localStorage.hiddenColumns = []
    }
        $localStorage.hiddenColumns.push(c)
    }

    function removeHiddenColumn(c) {
        $localStorage.hiddenColumns.splice($localStorage.hiddenColumns.indexOf(c), 1)
    }

    function getHiddenColumns() {
        var ret = $localStorage.hiddenColumns
	if(ret == null) {
	    ret = []
	}
	return ret;
    }

    function clearHiddenColumns() {
        $localStorage.hiddenColumns = []
    }

  return {
    setItems: setItems,
    getItems: getItems,
    markHiddenColumn: markHiddenColumn,
    getHiddenColumns: getHiddenColumns,
    removeHiddenColumn: removeHiddenColumn,
    clearHiddenColumns: clearHiddenColumns
}
});

myApp.service('configService', function($localStorage) {
    var defaults = {
                           pgUrl: "localhost",
                           baseQuery: "SELECT * FROM mapmatch_history H, sf_datasets D WHERE H.dataset_id = D.id LIMIT 10000",
                           selectedTargets: { DEFAULT_CONFIG : {"attributes": ["userid"],"lowMetrics": ["data_count_minutes"] }}}

    var hasAnalysisForConfig = false

    function clearAnalysis() { return hasAnalysisForConfig = false; }
    function hasAnalysis() { return hasAnalysisForConfig }
    function analysisReceived() { hasAnalysisForConfig = true; }

  function reset()
   {
                hasAnalysisForConfig = false
            $localStorage.$reset(defaults)
   }


   function resetSchema() {
       reset();
     }

        function clearSchema() {
         hasAnalysisForConfig = false
          $localStorage.selectedTargets = {}
          }

function markForBasicQuery() {
    $localStorage.markForBasicQuery = true
}


function unmarkForBasicQuery() {
    $localStorage.markForBasicQuery = false
}

  function setPostgresUrl(url) {
    $localStorage.pgUrl = url
  }



  function getPostgresUrl() { return $localStorage.pgUrl }

  function setBaseQuery(q) { $localStorage.baseQuery = q }

    function getBaseQuery() { return $localStorage.baseQuery }


  function addConfigIfNeeded(name, listname) {
    if(!(name in $localStorage.selectedTargets)) {
        $localStorage.selectedTargets[name] = {}
    }

    if(!(listname in $localStorage.selectedTargets[name])) {

        $localStorage.selectedTargets[name][listname] = []
    }
  }

  var addAttribute = function(configName, attr) {
      addConfigIfNeeded($localStorage.selectedTargets, "attributes")
      $localStorage.selectedTargets[name][listname].push(attr)
  };

  function toggleListElement(l, e) {
      if(l.indexOf(e) < 0) {
        l.push(e)
      } else {
        l.splice(l.indexOf(e), 1);
      }
  }

    var toggleCollection = function(configName, collection, attr) {
        addConfigIfNeeded(configName, collection)
        toggleListElement($localStorage.selectedTargets[configName][collection], attr)
    };


    var getCollection = function(configName, collection) {
        if(!(configName in $localStorage.selectedTargets)) {
            return []

            }

        else if(!(collection in $localStorage.selectedTargets[configName])) {
            return []
            }
        else {
            return $localStorage.selectedTargets[configName][collection]
            }
    };

  var toggleAttribute = function(configName, attr) {
        toggleCollection(configName, "attributes", attr)
  };


    var toggleHighMetric = function(configName, attr) {
          toggleCollection(configName, "highMetrics", attr)
    };

        var toggleLowMetric = function(configName, attr) {
              toggleCollection(configName, "lowMetrics", attr)
        };

    var getAttributes = function(configName) {
        return getCollection(configName, "attributes")
    }


    var getHighMetrics = function(configName) {
        return getCollection(configName, "highMetrics")
    }


    var getLowMetrics = function(configName) {
        return getCollection(configName, "lowMetrics")
    }

  return {
    getAttributes: getAttributes,
    getHighMetrics: getHighMetrics,
    getLowMetrics: getLowMetrics,
    toggleAttribute: toggleAttribute,
    toggleHighMetric: toggleHighMetric,
    toggleLowMetric: toggleLowMetric,
    setBaseQuery: setBaseQuery,
    getBaseQuery: getBaseQuery,
    setPostgresUrl: setPostgresUrl,
    getPostgresUrl: getPostgresUrl,
    reset: reset,
    resetSchema: resetSchema,
    hasAnalysis: hasAnalysis,
    clearSchema: clearSchema,
    clearAnalysis: clearAnalysis,
    analysisReceived: analysisReceived,
      markForBasicQuery: markForBasicQuery,
      unmarkForBasicQuery: unmarkForBasicQuery,
  };

});


myApp.controller('connectController', ['$scope', '$http', '$window', 'configService', 'explorerService', function($scope, $http, $window, configService, explorerService) {


    $scope.updateDB = function() {
        $scope.get_schema(false)
    };

    $scope.get_schema = function(init) {
    if(init){
        $scope.baseQuery = configService.getBaseQuery()
    } else {
        configService.clearSchema()
        configService.setPostgresUrl($scope.postgresstr)
        configService.setBaseQuery($scope.baseQuery)
    	console.log("BASEQUERY:" +$scope.baseQuery)
    	console.log("PGSTR:"+ $scope.postgresstr)
    	console.log("PGSTRURL:"+ configService.getPostgresUrl())
    }

	$http.put("http://localhost:8080/api/schema",
	    {
    	        pgUrl: configService.getPostgresUrl(),
    	        baseQuery: configService.getBaseQuery() })
	    .then(function(response) {

	    if(!init)
	        configService.clearSchema();

        response.data.columns.sort(function (c1, c2) { return c1.name.localeCompare(c2.name); })
	    $scope.schemaCols = response.data;
	    $scope.pg_url = $scope.postgresstr;
        $scope.spice = $scope.postgresstr;
	    });
    };

    $scope.sampleRows = function() { 
        explorerService.setItems(null)
	configService.markForBasicQuery()
        $window.open("explore.html", "_blank")
    }

      $scope.resetSchema = function() { configService.resetSchema(); $scope.updateDB(); }
      $scope.clearSchema = function() { configService.clearSchema(); }


}]);


function selectorButton(m, l) {
    if(l.indexOf(m) > -1) {
	return "btn btn-default active-schema"
    } else {
	return "btn btn-default"
    }
}

var DEFAULT_CONFIG = "DEFAULT_CONFIG"

myApp.controller('selectorController', ['$scope', 'configService', function($scope, configService) {

    $scope.selectedMetric = function(metricName, isHigh) {
        if(isHigh)
	        configService.toggleHighMetric(DEFAULT_CONFIG, metricName)
	    else
   	        configService.toggleLowMetric(DEFAULT_CONFIG, metricName)
    }

    $scope.selectedAttribute = function(attributeName) {
	    configService.toggleAttribute(DEFAULT_CONFIG, attributeName)
    }

    $scope.applyClassAttributeGlyph = function(attributeName) {
	    if(configService.getAttributes(DEFAULT_CONFIG).indexOf(attributeName) > -1) {
    	    return "glyphicon glyphicon-ok"
        } else {
    	    return "glyphicon glyphicon-plus"
        }
    }

 $scope.applyClassAttributeButton = function(attributeName) {
	return selectorButton(attributeName, configService.getAttributes(DEFAULT_CONFIG))
    }

 $scope.applyClassMetricButton = function(metricName, isHigh) {
    if(isHigh)
	    return selectorButton(metricName, configService.getHighMetrics(DEFAULT_CONFIG))
	else
        return selectorButton(metricName, configService.getLowMetrics(DEFAULT_CONFIG))
    }
}]);


myApp.controller('analyzeController', ['$scope', '$http', '$window', 'configService', 'explorerService', function($scope, $http, $window, configService, explorerService) {

    var analyzing = false

    $scope.shouldShowAnalysis = function() {
        return configService.hasAnalysis()
    }

    $scope.triggerAnalyze = function() {

    if(!analyzing) {
        analyzing = true;
        configService.clearAnalysis();
        $scope.analyzeStr = "Analyzing..."

	$http.post("http://localhost:8080/api/analyze",
	    {
    	        pgUrl: configService.getPostgresUrl(),
    	        baseQuery: configService.getBaseQuery(),
    	        attributes: configService.getAttributes(DEFAULT_CONFIG),
    	        highMetrics: configService.getHighMetrics(DEFAULT_CONFIG),
    	        lowMetrics: configService.getLowMetrics(DEFAULT_CONFIG)
    	}, {timeout: 100000000 }
    )
	    .then(function(response) {
	        analyzing = false;
	        configService.analysisReceived()
	        $scope.analyzeStr = "Analyze"

            $scope.analysisResult = response.data;

            $scope.numOutliers = response.data.numOutliers
            $scope.numInliers = response.data.numInliers
            $scope.loadTime = response.data.loadTime
            $scope.executionTime = response.data.executionTime
            $scope.summarizationTime = response.data.summarizationTime
            $scope.itemsets = response.data.itemSets

            $scope.sortAnalysis("support");
	    });
    }
    }

    $scope.sortAnalysis = function(field) {
        $scope.itemsets.sort(function(a, b) {
            if(field === "itemSize") {
               return a.items.length - b.items.length;
            } else {
                var compare = b[field] - a[field];
                if(compare == 0) {
                    compare = b.items.length - a.items.length;
                    if(compare == 0) {
                        compare = b.items[0].value < a.items[0].value;
                    }
                }

                return compare;
            }
        });
    }

    $scope.exploreItems = function(items) {
        explorerService.setItems(items)
        $window.open("explore.html", "_blank")
    }
}]);


myApp.controller('exploreController', ['$scope', '$http', 'configService', 'explorerService', function($scope, $http, configService, explorerService) {

    $scope.visibleRows = []
    $scope.exploreRows = []

    $scope.itemset = angular.fromJson(explorerService.getItems())

    function updateVisibleData() {

	$scope.hasItemsets = function() {
	    console.log(explorerService.getItems())
	    return (explorerService.getItems() != "null")
	}

        $scope.visibleRows = angular.copy($scope.exploreRows)
         for(var row of $scope.visibleRows) {
                for(var hc of explorerService.getHiddenColumns()) {
                            for(var i=0; i < row.columnValues.length; i++) {

                    var ele = row.columnValues[i]

                    if(ele.column == hc) {

                        if(row.columnValues.length > 1)
                            row.columnValues.splice(i, 1)
                        else {
                            row.columnValues = []
                            }
                    }
                }
            }
        }
    }

    $scope.getHiddenColumns = function() { return explorerService.getHiddenColumns() }

    $scope.showColumn = function(c) {
        explorerService.removeHiddenColumn(c)
        updateVisibleData()
    }

    $scope.hideColumn = function(c) {
        explorerService.markHiddenColumn(c)
        updateVisibleData()
    }

    $scope.clearHiddenColumns = function () {
    explorerService.clearHiddenColumns()
     updateVisibleData();
    }

    $scope.hideAllColumns = function () {
        for(var p of $scope.exploreRows[0].columnValues) {
            if(explorerService.getHiddenColumns().indexOf(p.column) < 0) {
                explorerService.markHiddenColumn(p.column)
            }

        }
        updateVisibleData();
    }

    var rowNo = 0

    $scope.getRows = function() {

	configService.unmarkForBasicQuery()

	var _items = angular.fromJson(explorerService.getItems())
	if(_items == null) {
	    _items = []
	}
	 

	$http.post("http://localhost:8080/api/rows",
	    {
    	        pgUrl: configService.getPostgresUrl(),
                baseQuery: configService.getBaseQuery(),
                // silly, but okay
    	        columnValues: _items,
    	        limit: 10,
    	        offset: rowNo
    	}
    )
	    .then(function(response) {
	        $scope.headers = explorerService.getItems()
	        $scope.exploreRows = $scope.exploreRows.concat(response.data.rows)
	        $scope.visibleRows = angular.copy($scope.exploreRows)
	        rowNo += 10
	        updateVisibleData()
	    });
    }

}]);











