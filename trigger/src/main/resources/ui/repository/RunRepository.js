let RunRepository = new function () {

    this.getRuns = function (jobDefinitionId, model) {
        WSClient.get(
            "jobInstances?jobDefinitionId="+jobDefinitionId,
            function(data) {
                model.setProperty("/runs", data);
            },
            function() {
                new sap.m.MessageToast.show("Error while loading runs", {animationDuration: 5000});
            }
        );
    };

    this.getOverallStatistics = function (model) {
        WSClient.get(
            "jobInstances/overallStatistics",
            function(data) {
                model.setProperty("/overallStatistics", data);
            },
            function() {
                new sap.m.MessageToast.show("Error while loading statistics", {animationDuration: 5000});
            }
        );
    };

    this.getPerWorkflowStatistics = function (model) {
        WSClient.get(
            "jobInstances/perWorkflowStatistics",
            function(data) {
                model.setProperty("/runs", data);
            },
            function() {
                new sap.m.MessageToast.show("Error while loading runs", {animationDuration: 5000});
            }
        );
    };
}();