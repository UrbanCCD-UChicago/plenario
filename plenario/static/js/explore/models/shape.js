var app = app || {};

app.Shape = Backbone.Model.extend({
    idAttribute:'dataset_name',
    initialize: function() {
        console.log("creating a shape");
    },
});