sap.ui.define([
	"sap/ui/core/mvc/Controller",
	"sap/ui/core/UIComponent",
	"sap/ui/core/routing/History"
], function(Controller, UIComponent, History) {
	"use strict";

	return Controller.extend("hyperdriver.controller.BaseController", {

		getRouter : function () {
			return UIComponent.getRouterFor(this);
		},

		getModel : function (sName) {
			return this.getView().getModel(sName);
		},

		setModel : function (oModel, sName) {
			return this.getView().setModel(oModel, sName);
		},

		myNavBack: function (sRoute, mData) {
			let oHistory = History.getInstance();
			let sPreviousHash = oHistory.getPreviousHash();

			if (sPreviousHash !== undefined) {
				history.go(-1);
			} else {
				this.getRouter().navTo("home");
			}
		}

	});

});