sap.ui.define([
	'./BaseController',
	"sap/ui/core/Fragment",
	"sap/m/MessageBox"
], function (BaseController, Fragment, MessageBox) {
	"use strict";
	return BaseController.extend("hyperdriver.controller.Workflows", {

		onInit: function () {
			this.getView().setModel(new sap.ui.model.json.JSONModel());
			this._model = this.getView().getModel();
			this.getRouter().attachRouteMatched(this.onViewDisplay, this);
		},

		onViewDisplay : function (evt) {
			evt.getParameter("name") === "workflows" && this.loadWorkflows();
		},

		onWorkflowAction: function (oEv) {
			let sAction = oEv.getParameter("item").data("action");
			let sId = oEv.getParameter("item").data("id");

			switch (sAction) {
				case "updateActiveStatus":
					WorkflowRepository.updateWorkflowActiveState(
						sId, {isActive: !oEv.getParameter("item").data("isActive")}
					);
					this.loadWorkflows();
					break;
				case "edit":
					this.getRouter().navTo("upsertWorkflow", {id: sId});
					break;
				case "delete":
					this.createConfirmDeleteWorkflowDialog(sId);
					break;
			}
		},

		onCreateWorkflow: function () {
			this.getRouter().navTo("upsertWorkflow");
		},

		loadWorkflows: function () {
			WorkflowRepository.getWorkflows(this._model);
		},

		createConfirmDeleteWorkflowDialog: function (id) {
			MessageBox.confirm("Are you sure you want to delete workflow ?", {
				icon: MessageBox.Icon.WARNING,
				actions: [sap.m.MessageBox.Action.YES, sap.m.MessageBox.Action.NO],
				onClose: function (oResponse) {
					if (oResponse === sap.m.MessageBox.Action.YES) {
						WorkflowRepository.deleteWorkflow(id);
						this.loadWorkflows()
					}
				}.bind(this)
			});
		},

		onWorkflowsRefresh: function () {
			this.loadWorkflows();
		}

	});
});