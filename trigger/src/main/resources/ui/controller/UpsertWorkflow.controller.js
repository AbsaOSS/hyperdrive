sap.ui.define([
	'./BaseController'
], function (BaseController) {
	"use strict";
	return BaseController.extend("hyperdriver.controller.UpsertWorkflow", {

		onInit: function () {
			this.getRouter().attachRouteMatched(this.onViewDisplay, this);
			this.getView().setModel(new sap.ui.model.json.JSONModel());
			this._model = this.getView().getModel();

			this._emptyJobParameters = {
				variables: {},
				maps: {}
			};
			this._emptyTriggerProperties = {
				properties: {
					variables: {},
					maps: {}
				},
				matchProperties: []
			};
			this._emptyWorkflow = {
				isActive: false,
					job: {
					jobType: {
						name: "Spark"
					},
					jobParameters: { ...this._emptyJobParameters }
				},
				trigger: {
					eventType: {
						name: "Absa-Kafka"
					},
					triggerProperties: {...this._emptyTriggerProperties }
				}
			}
		},

		onViewDisplay : function (evt) {
			if(evt.getParameter("name") === "upsertWorkflow") {
				let id = evt.getParameter("arguments").id;
				let isEdit = !!id;
				this._model.setProperty("/id", id);
				this._model.setProperty("/isEdit", isEdit);

				isEdit ? this.initEditDialog() : this.initCreateDialog();
				this._model.setProperty("/jobTypes", this.jobTypes);
				this._model.setProperty("/eventTypes", this.eventTypes);
			}
		},

		initEditDialog: function () {
			this._model.setProperty("/title", "Update");
			WorkflowRepository.getWorkflow(this._model.getProperty("/id"), this._model);
			this.loadViewFragments();
		},

		initCreateDialog: function () {
			this._model.setProperty("/title", "Create");
			this._model.setProperty("/workflow", jQuery.extend(true, {}, this._emptyWorkflow));
			this.loadViewFragments();
		},

		onSaveWorkflow: function () {
			let isEdit = this._model.getProperty("/isEdit");
			let workflow = this.getWorkflowToSave();
			if(isEdit) {
				WorkflowRepository.updateWorkflow(workflow);
			} else {
				WorkflowRepository.createWorkflow(workflow);
			}
			this.myNavBack();
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

		loadViewFragments: function () {
			this.onJobTypeSelect(true);
			this.onEventTypeSelect(true);
		},

        onJobTypeSelect: function (isInitial) {
			isInitial !== true && this._model.setProperty("/workflow/job/jobParameters", jQuery.extend(true, {}, this._emptyJobParameters));
			let key = this.getView().byId("jobTypeSelect").getSelectedKey();
			let fragmentName = this.jobTypes.find(function(e) { return e.name === key }).fragment;
            this.showFragmentInView(fragmentName, "hyperdriver.view.job", "jobForm")
        },

        onEventTypeSelect: function (isInitial) {
			isInitial !== true && this._model.setProperty("/workflow/trigger/triggerProperties", jQuery.extend(true, {}, this._emptyTriggerProperties));
			let key = this.getView().byId("eventTypeSelect").getSelectedKey();
            let fragmentName = this.eventTypes.find(function(e) { return e.name === key }).fragment;
            this.showFragmentInView(fragmentName, "hyperdriver.view.sensor", "sensorForm")
		},

        showFragmentInView: function (fragmentName, fragmentLocation, destination) {
            let oFragmentController = this._fragmentController[fragmentName];
            if (!oFragmentController) {
                let oController = eval("new " + fragmentName + "(this._model)");
                let oFragment = sap.ui.xmlfragment(fragmentName, fragmentLocation + "." + fragmentName, oController);
				oFragmentController = {fragment: oFragment, controller: oController};
                this._fragmentController[fragmentName] = oFragmentController;
            }

            let oLayout = this.getView().byId(destination);
            oLayout.removeAllContent();
			oFragmentController.fragment.forEach(oElement =>
                oLayout.addContent(oElement)
            );
			oFragmentController.controller.onShow();
        },

		onBackPress: function () {
			this.myNavBack();
		},

		onCancelWorkflow: function () {
			this.myNavBack();
		},

        _fragmentController: {},

		jobTypes: [
            {name: "Spark", fragment: "spark"}
		],

        eventTypes: [
            {name: "Absa-Kafka", fragment: "absaKafka"},
            {name: "Kafka", fragment: "kafka"}
        ]

	});
});