var app = app || {};

    app.ShapeView = Backbone.View.extend({

        el: '#shapes-view',
        // will go back to use template after fully integrate backbone with the original application
        //template:

        initialize: function() {
            var self = this;
            this.collection = new app.Shapes();
            this.collection.fetch({reset:true,success:function(){
                if (resp) {
                    self.query = resp.query;
                    self.setIntersection();
                }
            }
            });
            // if listen to reset, it will initialize a new collection and render the one without additional features
           this.listenTo(this.collection, 'all', this.render, this);
        },

        render: function(){
            var shapes;
            var intersect;
            var available;
            if (resp === undefined) {
                shapes = this.collection.toJSON();
                intersect = false;
                available = _.size(this.collection);
            } else {
                //filter out the shape datasets that intersect with the bounding box to display
                shapes = _.filter(this.collection.toJSON(), function(v){if(v.num_geoms) {return v};});
                intersect = true;
                available = _.size(_.filter(this.collection.pluck("num_geoms"), function(v) {return v !== undefined;}));
            }
            var template = template_cache('shapesList', {shapes:shapes, hasIntersect:intersect, available:available});
            this.$el.html(template);
            return this;
        },

        setIntersection: function(){
            var self = this;
            $.when(self.getIntersection()).then(
                function(resp) {
                    var data = resp.objects;
                    if (data.length > 0) {
                        data.forEach(function (intersect) {
                            self.collection.get(intersect.dataset_name).set("num_geoms", intersect.num_geoms);
                        });
                    }
                })
        },

        getIntersection: function(){
            var self = this;
            var q = self.getGeoJson();
            return $.ajax({
                url: '/v1/api/shapes/intersections/'+ q,
                dataType: 'json'
            });
        },

        getGeoJson: function() {
            var self = this;
            return self.query.location_geom__within;
        }
    });

