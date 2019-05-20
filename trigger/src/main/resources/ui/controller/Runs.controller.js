sap.ui.define([
	'./BaseController'
], function (BaseController) {
	"use strict";
	return BaseController.extend("hyperdriver.controller.Runs", {

		onInit: function () {
			this.getView().setModel(new sap.ui.model.json.JSONModel());
			this._model = this.getView().getModel();
			this.getRouter().attachRouteMatched(this.onViewDisplay, this);
		},

		onViewDisplay : function (evt) {
			evt.getParameter("name") === "runs" && this.loadRuns();
		},

		loadRuns: function () {
			RunRepository.getPerWorkflowStatistics(this._model);
		},

		onRunsRefresh: function () {
			this.loadRuns()
		},

		onRunsPress: function (oEvent) {
			this.getRouter().navTo("workflowRuns", {
				jobDefinitionId: oEvent.getSource().getBindingContext().getProperty("jobDefinitionId")
			});

		},

	});
});