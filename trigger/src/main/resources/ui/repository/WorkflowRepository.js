let WorkflowRepository = new function () {

    this.getWorkflows = function (model) {
        WSClient.get(
            "workflows",
            function(data) {
                model.setProperty("/workflows", data);
            },
            function() {
                new sap.m.MessageToast.show("Error while loading workflows", {animationDuration: 5000});
            }
        );
    };

    this.getWorkflow = function (id, model) {
        WSClient.get(
            "workflow?id="+id,
            function(data) {
                let res = [];
                let obj = data.trigger.triggerProperties.matchProperties;
                Object.keys(obj).map(k => res.push({ keyField: k, valueField: obj[k] }));
                data.trigger.triggerProperties.matchProperties = res;

                model.setProperty("/workflow", data);
            },
            function() {
                new sap.m.MessageToast.show("Error while loading workflow", {animationDuration: 5000});
            }
        );
    };

    this.deleteWorkflow = function (id) {
        WSClient.delete(
            "workflows?id="+id,
            function() {
                new sap.m.MessageToast.show("Workflow deleted", {animationDuration: 5000});
            },
            function() {
                new sap.m.MessageToast.show("Error while deleting workflow", {animationDuration: 5000});
            }
        );
    };

    this.createWorkflow = function (workflow) {
        WSClient.put(
            "workflow",
            function() {
                new sap.m.MessageToast.show("Workflow created", {animationDuration: 5000});
            },
            function() {
                new sap.m.MessageToast.show("Error while creating workflow", {animationDuration: 5000});
            },
            workflow
        );
    };

    this.updateWorkflow = function (workflow) {
        WSClient.post(
            "workflows",
            function() {
                new sap.m.MessageToast.show("Workflow updated", {animationDuration: 5000});
            },
            function() {
                new sap.m.MessageToast.show("Error while updating workflow", {animationDuration: 5000});
            },
            workflow
        );
    };

    this.updateWorkflowActiveState = function (id, workflowState) {
        WSClient.post(
            "workflows/"+id+"/setActiveState",
            function() {
                new sap.m.MessageToast.show("Workflow updated", {animationDuration: 5000});
            },
            function() {
                new sap.m.MessageToast.show("Error while updating workflow", {animationDuration: 5000});
            },
            workflowState
        );
    }

}();