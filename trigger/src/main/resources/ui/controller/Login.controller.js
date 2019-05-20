sap.ui.define([
  './BaseController'
], function (BaseController) {
  "use strict";

  const usernameField = "username";
  const passwordField = "password";

  return BaseController.extend("hyperdriver.controller.Login", {
    loginForm: {},

    onInit: function () {

    },

    onLoginSubmit: function (oEvent) {
      let oData = {
        username: this.byId(usernameField).getValue(),
        password: this.byId(passwordField).getValue()
      };

      this._resetLoginFormState();
      if (this._validateLogin(oData)) {
        this._login(oData, this.byId("loginSubmit"))
      }
    },

    _resetFieldState: function (sField) {
      this.byId(sField).setValueState(sap.ui.core.ValueState.None);
      this.byId(sField).setValueStateText("");
    },

    _resetLoginFormState: function () {
      this._resetFieldState(usernameField);
      this._resetFieldState(passwordField);
    },

    _validateField: function (oData, sField, sErrorMessage) {
      let isOk = oData[sField] && oData[sField] !== "";

      if (!isOk) {
        let field = this.byId(sField);
        field.setValueState(sap.ui.core.ValueState.Error);
        field.setValueStateText(sErrorMessage);
      }

      return isOk;
    },

    _validateLogin(oData) {
      let isValidUsername = this._validateField(oData, usernameField, "Username cannot be empty.");
      let isValidPassword = this._validateField(oData, passwordField, "Password cannot be empty.");
      return isValidUsername && isValidPassword;
    },

    _login: function (oData, oControl) {
      let fnSuccess = (result, status, xhr) => {
        let csrfToken = xhr.getResponseHeader("X-CSRF-TOKEN");
        localStorage.setItem("csrfToken", csrfToken);
        let fnSuccessGetUserInfo = (oInfo) => {
          sap.ui.getCore().getModel().setProperty("/userInfo", oInfo);
          this.myNavBack();
        };
        AuthRepository.getUserInfo(fnSuccessGetUserInfo)
      };

      let fnError = () => {
        this.byId(usernameField).setValueState(sap.ui.core.ValueState.Error);
        this.byId(passwordField).setValueState(sap.ui.core.ValueState.Error);
      };
      AuthRepository.login(
          oData,
          fnSuccess,
          fnError
      );
    },

  });

});