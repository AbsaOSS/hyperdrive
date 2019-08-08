class kafka {
    constructor(model) {
        this._model = model;
    }

    onShow() {}

    onDeleteServer(oEv) {
        UiListMethods.deleteListItem("/workflow/trigger/triggerProperties/properties/maps/servers", this._model, oEv)
    }

    onDeleteMatchProperty(oEv) {
        UiListMethods.deleteListItem("/workflow/trigger/triggerProperties/matchProperties", this._model, oEv)
    }

    onAddMatchProperty() {
        UiListMethods.addListItem( "/workflow/trigger/triggerProperties/matchProperties", this._model, {"keyField": "", "valueField": ""})
    }

    onAddServer() {
        UiListMethods.addListItem("/workflow/trigger/triggerProperties/properties/maps/servers", this._model, "")
    }

}