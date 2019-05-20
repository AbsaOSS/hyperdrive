sap.ui.define([
	"sap/ui/core/UIComponent"
], function(UIComponent) {
	"use strict";
	return UIComponent.extend("hyperdriver.Component", {
		metadata: {
			manifest: "json"
		},
		init: function () {
			// call the init function of the parent
			UIComponent.prototype.init.apply(this, arguments);

			// add SAP-icons-TNT icons
			sap.ui.core.IconPool.registerFont({
				fontFamily: "SAP-icons-TNT",
				collectionName: "SAP-icons-TNT",
				fontURI: sap.ui.require.toUrl("sap/tnt/themes/base/fonts"),
				lazy: true
			});

			// create the views based on the url/hash
			this.getRouter().initialize();
		},

		getContentDensityClass: function () {
			if (!this._sContentDensityClass) {
				if (!sap.ui.Device.support.touch){
					this._sContentDensityClass = "sapUiSizeCompact";
				} else {
					this._sContentDensityClass = "sapUiSizeCozy";
				}
			}
			return this._sContentDensityClass;
		}
	});
});