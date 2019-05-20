let WSClient = new function () {

    this.get = function (url, fnSuccess, fnError) {
        this._ajax("GET", url, fnSuccess, fnError, {});
    };

    this.put = function (url, fnSuccess, fnError, data) {
        this._ajax("PUT", url, fnSuccess, fnError, data);
    };

    this.post = function (url, fnSuccess, fnError, data) {
        this._ajax("POST", url, fnSuccess, fnError, data);
    };

    this.delete = function (url, fnSuccess, fnError) {
        this._ajax("DELETE", url, fnSuccess, fnError, {});
    };

    this._ajax = function (method, url, fnSuccess, fnError, data) {
        let fnUnauthorized = (xhr) => {
            if (xhr.status === 401) {
                sap.ui.getCore().getEventBus().publish("nav", "unauthorized");
            } else {
                fnError();
            }
        };
        let oFormattedData = null;
        if ((method.toLowerCase() === "post" || method.toLowerCase() === "put")
            && (typeof data === "object")) {
            oFormattedData = JSON.stringify(data)
        }
        $.ajax({
            beforeSend: (oJqXHR, oSettings) => {
                if (method.toLowerCase() !== "get") {
                    let csrfToken = localStorage.getItem("csrfToken");
                    oJqXHR.setRequestHeader("X-CSRF-TOKEN", csrfToken);
                }
            },
            type: method,
            url: url,
            contentType: "application/json",
            dataType: "json",
            data:  oFormattedData,
            async: false,
            success: fnSuccess,
            error: fnUnauthorized
        });
    }

}();
