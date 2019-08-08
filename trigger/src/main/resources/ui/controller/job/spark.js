class spark {
    constructor(model) {
        this._model = model;
    }

    onShow() {}

    onDeleteJobProperty(oEv) {
        UiListMethods.deleteListItem("/workflow/job/jobParameters/maps/appArguments", this._model, oEv)
    }

    onAddJobProperty() {
        UiListMethods.addListItem("/workflow/job/jobParameters/maps/appArguments", this._model, "")
    }

}