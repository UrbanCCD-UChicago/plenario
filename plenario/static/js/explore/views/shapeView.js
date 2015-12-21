var app = app || {};

    app.ShapeView = Backbone.View.extend({

        el: $('#shapes-list'),
        //template: template_cache('shapesList', )

        initialize: function() {
            console.log("hello shapeview");
            this.collection = new app.Shapes();
            this.collection.fetch({reset:true, validation:true});
            console.log(this.collection);
            // Listen for event from about-view
        },

        render: function(){
            template = template_cache('shapesList', this.collection.toJSON());
            el.html(template)
        }


    });