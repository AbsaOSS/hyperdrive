let AuthRepository = new function () {

    this.getUserInfo = function (fnSuccess) {
        $.ajax("user/info", {
            method: "GET",
            success: fnSuccess,
            async: false
        })
    };

    this.login = function (oData, fnSuccess, fnError) {
        $.ajax("login", {
            data: oData,
            method: "POST",
            success: fnSuccess,
            error: fnError,
            async: false
        })
    };

    this.logout = function () {
        $.ajax("logout", {
            method: "POST",
            beforeSend: (oJqXHR, oSettings) => {
                let csrfToken = localStorage.getItem("csrfToken");
                oJqXHR.setRequestHeader("X-CSRF-TOKEN", csrfToken);
            },
            async: false
        })
    };

}();
