var app = app || {};

    app.ShapeView = Backbone.View.extend({

        el: $('#shapes-list'),
        //template: template_cache('shapesList', )

        initialize: function() {
            console.log("hello shapeview");
            this.collection = new app.Shapes();
            this.on("reset", this.render);

            self = this;
            this.collection.fetch({reset: true});
            console.log(this.collection);
        },

        render: function(){
            console.log(this.collection.toJSON());
            template = template_cache('shapesList', {shapes: this.collection.toJSON()});
            this.$el.html(template)
        }


    });