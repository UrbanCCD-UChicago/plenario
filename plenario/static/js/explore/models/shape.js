var app = app || {};

app.Shape = Backbone.Model.extend({
    idAttribute:'dataset_name',
    initialize: function() {
        //console.log("initializing shape dataset: " + this.id);
    },
});