
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
                           baseQuery: "SELECT * FROM sensor_data;",
                           selectedTargets: { DEFAULT_CONFIG : {"attributes": ["state"],"highMetrics": ["power_drain"] }}}

    var hasAnalysisForConfig = false

    function clearAnalysis() { return hasAnalysisForConfig = false; }
    function hasAnalysis() { return hasAnalysisForConfig }
    function analysisReceived() { hasAnalysisForConfig = true; }

    function handleError(str) {
        $localStorage.errorStr = str;
        if (str) {
            console.log(str);
        }
    }

    function hasError() {
        if($localStorage.errorStr) {
            return true;
        }

        return false;
    }

    function getError() {
        return $localStorage.errorStr;
    }

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
    handleError: handleError,
    hasError: hasError,
    getError: getError,
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

	$http.put("api/schema",
	    {
    	        pgUrl: configService.getPostgresUrl(),
    	        baseQuery: configService.getBaseQuery() })
	    .then(function(response) {

        configService.handleError(response.data.errorMessage);

	    if(!init)
	        configService.clearSchema();

        response.data.schema.columns.sort(function (c1, c2) { return c1.name.localeCompare(c2.name); })
	    $scope.schemaCols = response.data.schema;
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

myApp.controller('errorController', ['$scope', 'configService', function($scope, configService) {
    $scope.errorStr = configService.getError();
    $scope.hasError = function() { return configService.hasError(); }
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

	$http.post("api/analyze",
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

        configService.handleError(response.data.errorMessage);

	    var result = response.data.results[0];

            $scope.analysisResult = result;

            $scope.numOutliers = result.numOutliers
            $scope.numInliers = result.numInliers
            $scope.loadTime = result.loadTime
            $scope.executionTime = result.executionTime
            $scope.summarizationTime = result.summarizationTime
            $scope.itemsets = result.itemSets

			$scope.resetPlotDiv();
			$scope.resetItemsetPlots();

            $scope.sortAnalysis("support");
	    });
    }
    }

    $scope.sortAnalysis = function(field) {
			$scope.resetPlotDiv();
			$scope.resetItemsetPlots();
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

    //get names of all columns that should be available to plot
    $scope.getPlotColumns = function() {
        var plotCols = [];
        plotCols = plotCols.concat(configService.getHighMetrics(DEFAULT_CONFIG));
        plotCols = plotCols.concat(configService.getLowMetrics(DEFAULT_CONFIG));
        return plotCols;
    }

    //variable relevant to top-level plot

    $scope.plotDataLoading = false; /* used to disable plotting related buttons
                                       during data load */


    $scope.plotPoints = [];
    $scope.plotField = null;
    $scope.hidePlot = true;
    $scope.plotCount = 10000; // upper bound on items to pull
    $scope.maxItemsetCount = 5; /* number of categories (including base) to
                                   make available in main plot */
    $scope.maxShownSeries = 3; /* number of outlier classes to show in main
                                  plot */

    //variable relevant to itemset-level plot
    $scope.itemsetPlotPoints = [];
    $scope.itemsetPlotField = [];
    $scope.itemsetHidePlot = [];

    $scope.plotDiv = document.getElementById('plotArea');

    //determines whether we should hide the itemset plot at index
    $scope.shouldHideItemsetPlot  = function (itemsetIdx){
        if(itemsetIdx > $scope.itemsetHidePlot.length){
            return true;
        }
        else{
            return $scope.itemsetHidePlot[itemsetIdx];
        }
    }

    // clear and hide itemset plots. called after analyze
    $scope.resetItemsetPlots  = function (){
        var data = [];
        $scope.itemsetHidePlot = [];
        for(var i = 0 ; i < $scope.itemsets.length; i++){
            $scope.itemsetHidePlot.push(true);
            Plotly.newPlot($scope.plotDiv, data, function(err,msg){
                console.log(msg);
            });
        }
    }

    // clear and hide top-level plot
    $scope.resetPlotDiv  = function (){
        var data = [];
        Plotly.newPlot($scope.plotDiv, data, function(err,msg){
            console.log(msg);
        });
        $scope.hidePlot = true
    }

    // update top-level plot with data from $scope.plotPoints
    $scope.updatePlotDiv = function (){
        var data = [];
        showSeries = true;

        for(var i = 0 ; i < $scope.plotPoints.length; i++){
            if(i > $scope.maxShownSeries){
                showSeries = "legendonly";
            }
            data.push({x:$scope.plotPoints[i][0],
                opacity:0.5, type:'histogram',
                histnorm:'probability density',
                name:$scope.plotPoints[i][1],
                visible:showSeries
            });
        }

        var layout = {
            xaxis: {title: $scope.plotField},
            yaxis: {title: 'Probability Density'},
            barmode:'overlay',
            bargap: 0.1,
            bargroupgap: 0.2,
            width:500,
            legend:{
              x:0,
              y:-0.6
            },
        };
        Plotly.newPlot($scope.plotDiv, data, layout, function (err, msg) {
            console.log(msg);
        });

        $scope.hidePlot = false;
    }

    // update plots for itemset subsection for given idx
    $scope.updateItemsetPlotDivs = function (itemsetIdx){
        var data = [];
        showSeries = true;

        if(itemsetIdx >= $scope.itemsetPlotPoints.length)
            return;

        var plotPoints = $scope.itemsetPlotPoints[itemsetIdx];

        itemsetPlotDiv = document.getElementById('plotArea_'+itemsetIdx);
        //plotPoints[0] is base and [1] is for class
        for(var i = 0 ; i <= 1; i++){
            data.push({x:plotPoints[i][0],
                opacity:0.5, type:'histogram',
                histnorm:'probability density',
                name:plotPoints[i][1],
                visible:showSeries
            });
        }
        var layout = {
            xaxis: {title: $scope.itemsetPlotField[itemsetIdx]},
            yaxis: {title: 'Probability Density'},
            barmode:'overlay',
            bargap: 0.1,
            bargroupgap: 0.2,
            width:500,
            legend:{
              x:0,
              y:-0.6
            },
        };
        Plotly.newPlot(itemsetPlotDiv, data, layout, function (err, msg) {
            console.log(msg);
        });

        $scope.itemsetHidePlot[itemsetIdx] = false;
    }

    //called when one of the plot all buttons is clicked
    $scope.prepareAllItemsetPlotData = function(fname) {
        for(var i = 0; i < $scope.itemsets.length; i++){
            $scope.prepareItemsetPlotData(fname, i);
        }
    }

    //first call used to populate one of the itemset plots
    $scope.prepareItemsetPlotData = function(fname, itemsetIdx) {
        $scope.itemsetPlotPoints[itemsetIdx] = [];
        $scope.itemsetPlotField[itemsetIdx] = fname;
        var fieldIdx = null;
        $scope.plotDataLoading = true;

        $scope.getItemsetPlotRows($scope.itemsets[itemsetIdx].items,
                itemsetIdx, fname, fieldIdx, true);
    }

    //hit api to get rows for itemset
    $scope.getItemsetPlotRows = function(items, itemsetIdx, fname, fieldIdx) {

        var colVals = [[]];

        if(items == null) {
            items = [];
        }

        colVals.push(items);

        $http.post("api/rows/multiple",
                {
                    pgUrl: configService.getPostgresUrl(),
            baseQuery: configService.getBaseQuery(),
            columnValues: colVals,
            limit: $scope.plotCount,
            offset: 0
                }
                ).then(function(response) {
                    configService.handleError(response.data.errorMessage);
                    $scope.prepareItemsetPlotDataPostBaseQuery(response.data.rowSets, fname, itemsetIdx)
                } , function(error){
                    $scope.plotDataLoading = false
                    console.log(error)
                });
    }

    $scope.prepareItemsetPlotDataPostBaseQuery = function( dataRows, fname, itemsetIdx) {

        var baseRows = dataRows[0].rows;
        var itemsetRows = dataRows[1].rows;

        if(baseRows.length == 0){
            $scope.plotDataLoading = false;
            console.error("Base data was empty for "+itemsetIdx);
            return;
        }

        //use first elem as template to ensure col is in fields
        var firstEle = baseRows[0];

        //ensure column is in list
        for(var i=0; i < firstEle.columnValues.length; i++) {
            var ele = firstEle.columnValues[i];

            if(ele.column == fname) {
                //set relevant vars
                $scope.plotField = fname;
                fieldIdx = i;
                break
            }
        }

        //invalid fname
        //TODO: raise error
        if ($scope.itemsetPlotField[itemsetIdx] == null){
            $scope.plotDataLoading = false;
            return;
        }

        plotPoints = $scope.itemsetPlotPoints[itemsetIdx];

        plotPoints.push([]);
        //load in rows if not yet loaded
        plotPoints[0][0] = [];
        plotPoints[0][1] = 'base';
        for(var row of baseRows) {
            plotPoints[0][0].push(row.columnValues[fieldIdx].value);
        }

        var name = $scope.createNameFromItemset($scope.itemsets[itemsetIdx].items);
        plotPoints = $scope.itemsetPlotPoints[itemsetIdx];
        plotPoints.push([]);
        plotPoints[1][0] = [];
        plotPoints[1][1] = name;

        for(var row of itemsetRows) {
            plotPoints[1][0].push(row.columnValues[fieldIdx].value);
        }

        $scope.updateItemsetPlotDivs(itemsetIdx);
        $scope.plotDataLoading = false;
    }

    //first call for populating top-level chart
    $scope.preparePlotData = function(fname) {
        $scope.plotPoints = [];
        $scope.plotField = null;
        $scope.plotDataLoading = true;

        $scope.getPlotRows(Math.min($scope.itemsets.length,$scope.maxItemsetCount),
                fname);
    }

    $scope.preparePlotDataPostBaseQuery = function( dataRows, fname) {

        var baseRows = dataRows[0].rows;
        var fieldIdx = null;

        if(baseRows.length == 0){
            $scope.plotDataLoading = false;
            return;//TODO: show message about missing data
        }

        //use first elem as template to ensure col is in fields
        var firstEle = baseRows[0];

        //ensure column is in list
        for(var i=0; i < firstEle.columnValues.length; i++) {
            var ele = firstEle.columnValues[i];

            if(ele.column == fname) {
                //set relevant vars
                $scope.plotField = fname;
                fieldIdx = i;
                break;
            }
        }

        //invalid fname
        //TODO: raise error
        if ($scope.plotField == null){
            $scope.plotDataLoading = false
                return;
        }


        $scope.plotPoints.push([])
            //load in rows if not yet loaded
            $scope.plotPoints[0][0] = []
            $scope.plotPoints[0][1] = 'base'
            for(var row of baseRows) {
                $scope.plotPoints[0][0].push(row.columnValues[fieldIdx].value)
            }

        for(var i = 1; i < dataRows.length; i++){
            var itemsetRows = dataRows[i].rows;
            var name = $scope.createNameFromItemset($scope.itemsets[i-1].items);
            $scope.plotPoints.push([]);
            $scope.plotPoints[i][0] = [];
            $scope.plotPoints[i][1] = name;
            for(var row of itemsetRows) {
                $scope.plotPoints[i][0].push(row.columnValues[fieldIdx].value);
            }
        }
        $scope.updatePlotDiv();
        $scope.plotDataLoading = false;
    }

        //create name for series given items
    $scope.createNameFromItemset = function(items){
        var names = []
            for(var item of items){
                names.push(item.column+":"+item.value)

            }
        return names.join()
    }

    //get rows for top-level plot from API
    $scope.getPlotRows = function(maxCount, fname) {

        configService.unmarkForBasicQuery();

        //empty array for base
        colVals = [[]];

        for(var i=0; i < Math.min(maxCount, $scope.itemsets.length); i++){

            var itemset = $scope.itemsets[i];

            if(itemset.items == null) {
                colVals.push([]);
            } else{
                colVals.push(itemset.items);
            }
        }


        $http.post("api/rows/multiple",
                { pgUrl: configService.getPostgresUrl(),
                    baseQuery: configService.getBaseQuery(),
            columnValues: colVals,
            limit: $scope.plotCount,
            offset: 0
                }).then(function(response) {
                    configService.handleError(response.data.errorMessage);
                    $scope.preparePlotDataPostBaseQuery(response.data.rowSets, fname);
                } , function(error){
                    $scope.plotDataLoading = false;
                    console.log(error);
                });
    }

}]);


myApp.controller('exploreController', ['$scope', '$http', 'configService', 'explorerService', function($scope, $http, configService, explorerService) {

    $scope.visibleRows = []
    $scope.exploreRows = []

    $scope.itemset = angular.fromJson(explorerService.getItems())

    //plotting related vars
	$scope.plotPoints = [];
	$scope.plotField = null;
	$scope.plotCount = 100000;
	$scope.plotDataLoading = false;

	$scope.plotDiv = document.getElementById('plotArea');

    //update plots given data in plotPoints
    $scope.updatePlotDiv = function (){
        data=[];
        data.push({x:$scope.plotPoints[0],
            opacity:0.75,
            type:'histogram',
            histnorm:'probability density',
            name:'base'});

        if($scope.plotPoints.length > 0){
            data.push({x:$scope.plotPoints[1],
                opacity:0.75,
                type:'histogram',
                histnorm:'probability density',
                name:'selected subgroup'});
        }
        var layout = {
            xaxis: {title: $scope.plotField},
            yaxis: {title: 'Probability Density'},
            barmode:'overlay'};
        Plotly.newPlot($scope.plotDiv, data=data, layout, function (err, msg) {
            console.log(msg);
        });
    }



    function updateVisibleData () {

        $scope.hasItemsets = function() {
            console.log(explorerService.getItems())
                return (explorerService.getItems() != "null")
        }

        $scope.visibleRows = angular.copy($scope.exploreRows);
        for(var row of $scope.visibleRows) {
            for(var hc of explorerService.getHiddenColumns()) {
                for(var i=0; i < row.columnValues.length; i++) {

                    var ele = row.columnValues[i];

                    if(ele.column == hc) {

                        if(row.columnValues.length > 1)
                            row.columnValues.splice(i, 1);
                        else {
                            row.columnValues = [];
                        }
                    }
                }
            }
        }
    }

    $scope.getHiddenColumns = function() {
        return explorerService.getHiddenColumns() }

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


	$http.post("api/rows",
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
            configService.handleError(response.data.errorMessage);
	        $scope.exploreRows = $scope.exploreRows.concat(response.data.rowSet.rows)
	        $scope.visibleRows = angular.copy($scope.exploreRows)
	        rowNo += 10
	        updateVisibleData()
	    });
    }

    //should we show the plot?
    shouldShowPlot = function(){
                    return $scope.plotField == null;
    };

    //called by plot buttons
    $scope.preparePlotData = function(fname) {
        $scope.plotPoints = []
            $scope.plotField = null
            $scope.plotDataLoading = true

            $scope.getPlotRows(fname)
    }

    //use data to populate plots
    $scope.generatePlotData = function(fname, plotRows){
        var fieldIdx = null;

        var baseRows = plotRows[0];

        if(baseRows.length == 0){
            console.error("Base Query has no data");
            return;
        }

        //use first elem as template to ensure col is in fields
        var firstEle = baseRows[0];

        //ensure column is in list
        for(var i=0; i < firstEle.columnValues.length; i++) {
            var ele = firstEle.columnValues[i];

            if(ele.column == fname) {
                //set relevant vars
                $scope.plotField = fname;
                fieldIdx = i;
                break
            }
        }

        //invalid fname
        //TODO: raise error
        if ($scope.plotField == null)
            return;

        for(var i = 0; i < plotRows.length; i++) {
            for(var row of plotRows[i]) {
                $scope.plotPoints.push([]);
                $scope.plotPoints[i].push(row.columnValues[fieldIdx].value);
            }
        }

        $scope.updatePlotDiv();
        $scope.plotDataLoading = false;

    }

    //get rows for plot from API
    $scope.getPlotRows = function(fname) {

        configService.unmarkForBasicQuery();

        var colVals = [[]];

        var _items = angular.fromJson(explorerService.getItems());
        if(_items != null) {
            colVals.push(_items);
        }



        $http.post("api/rows/multiple",
                {
                    pgUrl: configService.getPostgresUrl(),
                    baseQuery: configService.getBaseQuery(),
                    columnValues: colVals,
                    limit: $scope.plotCount,
                    offset: 0
                })
            .then(function(response) {
                plotRows = []
                    configService.handleError(response.data.errorMessage);

                for(var i = 0; i < response.data.rowSets.length; i++){
                    plotRows.push(response.data.rowSets[i].rows);
                }
            $scope.generatePlotData(fname, plotRows);
            }, function(error){
                $scope.plotDataLoading = false;
                console.log(error);
            });
    }
}]);