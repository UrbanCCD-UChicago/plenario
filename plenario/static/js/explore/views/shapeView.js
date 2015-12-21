var app = app || {};

    app.ShapeView = Backbone.View.extend({

        el: $('#shapes-list'),
        //template:

        initialize: function() {
          this.collection = new app.Shapes();
          this.collection.fetch({reset:true});
          this.render();
          this.listenTo(this.collection, 'reset', this.render);
        },

        render: function(){
            template = template_cache('shapesList', {shapes: this.collection.toJSON()});
            this.$el.html(template)
        }


    });