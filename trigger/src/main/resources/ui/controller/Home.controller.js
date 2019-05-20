sap.ui.define([
	'./BaseController',
	'../lib/openui5-chartjs-library-preload'
], function (BaseController, ChartJS) {
	"use strict";
	return BaseController.extend("hyperdriver.controller.Home", {

		onInit: function () {
			this.getView().setModel(new sap.ui.model.json.JSONModel());
			this._model = this.getView().getModel();
			this.getRouter().attachRouteMatched(this.onViewDisplay, this);
			this.loadStatistics();
		},

		onViewDisplay : function (evt) {
			evt.getParameter("name") === "home" && this.loadStatistics();
		},

		onRunsRefresh: function () {
			this.loadStatistics();
			this.byId("successRateChart").updateChart();
			this.byId("runningQueuedChart").updateChart();
		},

		loadStatistics: function () {
			RunRepository.getOverallStatistics(this._model);
			let successful = this._model.getProperty("/overallStatistics/successful");
			let failed = this._model.getProperty("/overallStatistics/failed");
			let running = this._model.getProperty("/overallStatistics/running");
			let queued = this._model.getProperty("/overallStatistics/queued");
			this.updateChartsData(successful, failed, running, queued);
		},

		updateChartsData: function (successful, failed, running, queued) {
			successful === 0 && failed === 0
				? this._model.setProperty("/successRate", jQuery.extend(true, {}, this.getNoData()))
				: this._model.setProperty("/successRate", jQuery.extend(true, {}, this.getSuccessRateData(successful, failed)));
			running === 0 && queued === 0
				? this._model.setProperty("/runningQueued", jQuery.extend(true, {}, this.getNoData()))
				: this._model.setProperty("/runningQueued", jQuery.extend(true, {}, this.getRunningQueuedData(running, queued)));
		},

		getNoData: function() {
			return {
				labels: ["No runs"],
				datasets: [{
					backgroundColor: ['#D3D3D3'],
					data: [100]
				}]
			};
		},

		getSuccessRateData: function(successful, failed) {
			return {
				labels: ["Successful", "Failed"],
					datasets: [{
					data: [successful, failed],
					backgroundColor: ["#1d9c65", "#e05e2e"],
				}]
			};
		},

		getRunningQueuedData: function(running, queued) {
			return {
				labels: ["Running", "Queued"],
				datasets: [{
					data: [running, queued],
					backgroundColor: ["#4885d1", "#8143c4"],
				}]
			};
		}

	});
});