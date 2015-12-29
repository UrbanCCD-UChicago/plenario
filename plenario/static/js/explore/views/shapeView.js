var app = app || {};

    app.ShapeView = Backbone.View.extend({

        el: '#shapes-view',
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
            var shapes = this.collection.toJSON();
            var intersect;
            var available;
            if (resp === undefined) {
                intersect = false;
                available = _.size(this.collection);
            } else {
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
                }).fail(function(resp){
                    var error = {
                        header: 'Woops!',
                        body: "Error fetching data.",
                    }
                    new ErrorView({el: '#errorModal', model: error});
                });
        },

        getIntersection: function(){
            var self = this;
            var q = self.getGeoJson();
            return $.ajax({
                //url: 'http://plenar.io/v1/api/shapes/intersections/'+ q,
                url: '/v1/api/shapes/intersections/'+ q,
                //url: 'http://plenar.io/v1/api/shapes/intersections/{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[-87.67248630523682,41.86454328565965],[-87.67248630523682,41.872117384500754],[-87.6549768447876,41.872117384500754],[-87.6549768447876,41.86454328565965],[-87.67248630523682,41.86454328565965]]]}}',
                dataType: 'json',
            });
        },
        getGeoJson: function() {
            var self = this;
            return self.query.location_geom__within;
        }
    });

