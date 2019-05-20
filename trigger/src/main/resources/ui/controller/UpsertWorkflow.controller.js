sap.ui.define([
	'./BaseController'
], function (BaseController) {
	"use strict";
	return BaseController.extend("hyperdriver.controller.UpsertWorkflow", {

		onBackPress: function () {
			this.myNavBack();
		},

		onInit: function () {
			this.getView().setModel(new sap.ui.model.json.JSONModel());
			this._model = this.getView().getModel();
			this.getRouter().attachRouteMatched(this.onViewDisplay, this);
		},

		onViewDisplay : function (evt) {
			if(evt.getParameter("name") === "upsertWorkflow") {
				this.getView().setModel(new sap.ui.model.json.JSONModel());
				this._model = this.getView().getModel();

				let id = evt.getParameter("arguments").id;
				let isEdit = !!id;
				this._model.setProperty("/isEdit", isEdit);
				this._model.setProperty("/id", id);

				this._model.getProperty("/isEdit") ? this.initEditDialog() : this.initCreateDialog();
				this._model.setProperty("/jobTypes", this.jobTypes);
				this._model.setProperty("/eventTypes", this.eventTypes);
				if(isEdit && !this._model.getProperty("/workflow/name")) {
					this.myNavBack();
				}
			}
		},

		initEditDialog: function () {
			this._model.setProperty("/title", "Update");
			WorkflowRepository.getWorkflow(this._model.getProperty("/id"), this._model);
		},

		initCreateDialog: function () {
			this._model.setProperty("/workflow", jQuery.extend(true, {}, this._emptyWorkflow));
			this._model.setProperty("/title", "Create");
		},

		onSaveWorkflow: function () {
			let isEdit = this._model.getProperty("/isEdit");
			let workflow = this.getWorkflowToSave();
			if(isEdit){
				WorkflowRepository.updateWorkflow(workflow);
				this.myNavBack();
			} else {
				WorkflowRepository.createWorkflow(workflow);
				this.myNavBack();
			}
		},

		getWorkflowToSave: function () {
			let workflow = this._model.getProperty("/workflow");
			let matchProperties = {};
			workflow.trigger.triggerProperties.matchProperties.map(function(joinCondition) {
				matchProperties[joinCondition.keyField] = joinCondition.valueField
			});
			workflow.trigger.triggerProperties.matchProperties = matchProperties;

			return workflow
		},

		onAddMatchProperty: function () {
			let matchPropertiesPath = "/workflow/trigger/triggerProperties/matchProperties";
			let currentMatchProperties = this._model.getProperty(matchPropertiesPath);
			currentMatchProperties.push({
				"keyField": "",
				"valueField": ""
			});
			this._model.setProperty(matchPropertiesPath, currentMatchProperties);
		},

		onAddServer: function () {
			let matchServersPath = "/workflow/trigger/triggerProperties/properties/maps/servers";
			let currentServers = this._model.getProperty(matchServersPath);
			currentServers ? currentServers.push("") : currentServers = [""];
			this._model.setProperty(matchServersPath, currentServers);
		},

		onDeleteServer: function (oEv) {
			let matchServersPath = "/workflow/trigger/triggerProperties/properties/maps/servers";
			let toks = oEv.getParameter("listItem").getBindingContext().getPath().split("/");
			let inputColumnIndex = parseInt(toks[toks.length - 1]);
			let currentServers = this._model.getProperty(matchServersPath);
			let newServers = currentServers.filter((_, index) => index !== inputColumnIndex);
			this._model.setProperty(matchServersPath, newServers);
		},

		onDeleteMatchProperty: function (oEv) {
			let matchPropertiesPath = "/workflow/trigger/triggerProperties/matchProperties";
			let toks = oEv.getParameter("listItem").getBindingContext().getPath().split("/");
			let inputColumnIndex = parseInt(toks[toks.length - 1]);
			let currentMatchProperties = this._model.getProperty(matchPropertiesPath);
			let newMatchProperties = currentMatchProperties.filter((_, index) => index !== inputColumnIndex);
			this._model.setProperty(matchPropertiesPath, newMatchProperties);
		},

		onAddJobProperty: function () {
			let matchPropertiesPath = "/workflow/job/jobParameters/maps/appArguments";
			let currentMatchProperties = this._model.getProperty(matchPropertiesPath);
			currentMatchProperties ? currentMatchProperties.push("") : currentMatchProperties = [""];
			this._model.setProperty(matchPropertiesPath, currentMatchProperties);
		},

		onDeleteJobProperty: function (oEv) {
			let matchPropertiesPath = "/workflow/job/jobParameters/maps/appArguments";
			let toks = oEv.getParameter("listItem").getBindingContext().getPath().split("/");
			let inputColumnIndex = parseInt(toks[toks.length - 1]);
			let currentMatchProperties = this._model.getProperty(matchPropertiesPath);
			let newMatchProperties = currentMatchProperties.filter((_, index) => index !== inputColumnIndex);
			this._model.setProperty(matchPropertiesPath, newMatchProperties);
		},

		onCancelWorkflow: function () {
			this.myNavBack();
		},

		_emptyWorkflow: {
			"isActive": false,
			"job":{
				"jobType": {
					"name": "Spark"
				},
				"jobParameters": {
					"variables": {},
					"maps": {}
				}
			},
			"trigger": {
				"eventType": {
					"name": "Kafka"
				},
				"triggerProperties": {
					"properties": {
						"variables": {},
						"maps": {}
					},
					"matchProperties": []
				}
			}
		},

		jobTypes: [
			{type: "Spark"}
		],

		eventTypes: [
			{type: "Kafka"}
		]

	});
});