let UiListMethods = new function () {

    this.deleteListItem = function (path, model, event) {
        let tokens = event.getParameter("listItem").getBindingContext().getPath().split("/");
        let inputColumnIndex = parseInt(tokens[tokens.length - 1]);
        let currentElements = model.getProperty(path);
        let newElements = currentElements.filter((_, index) => index !== inputColumnIndex);
        model.setProperty(path, newElements);
    };

    this.addListItem = function (path, model, newElement) {
        let currentElements = model.getProperty(path);
        currentElements ? currentElements.push(newElement) : currentElements = [newElement];
        model.setProperty(path, currentElements);
    }

}();