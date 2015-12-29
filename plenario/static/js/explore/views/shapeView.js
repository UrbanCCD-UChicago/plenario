var app = app || {};

    app.ShapeView = Backbone.View.extend({

        el: '#shapes-view',
        //template:
        initialize: function() {
            console.log(this);
            var self = this;
            this.collection = new app.Shapes();
            this.collection.fetch({reset:true,success:function(){
                if (resp) {
                self.resp = resp;
                self.query = self.resp.query;
                self.setIntersection();
                }
            }
            });

            // if listen to reset, it will initialize a new collection and render the one without additional features
           this.listenTo(this.collection, 'all', this.render, this);
        },

        render: function(){
            console.log("Rendering");
            var shapes = this.collection.toJSON();
            var intersect;
            if (resp === undefined) {
                intersect = false;
            } else {
                intersect = true;
            }
            var template = template_cache('shapesList', {shapes:shapes,hasIntersect:intersect});
            this.$el.html(template);
            return this;
        },

        setIntersection: function(){
            var self = this;
            $.when(self.getIntersection()).then(
                function(resp) {
                    //var data = {
                    //    "meta": {"status": "ok", "message": ""},
                    //    "objects": [{
                    //        "dataset_name": "chicago_pedestrian_streets",
                    //        "num_geoms": 2
                    //    }, {
                    //        "dataset_name": "chicago_city_limits",
                    //        "num_geoms": 1
                    //    }, {
                    //        "dataset_name": "chicago_tif_districts",
                    //        "num_geoms": 2
                    //    }, {
                    //        "dataset_name": "chicago_wards",
                    //        "num_geoms": 9
                    //    }, {"dataset_name": "chicago_major_streets", "num_geoms": 28}]};
                    var data = resp.objects;
                    data.objects.forEach(function (intersect) {
                        self.collection.get(intersect.dataset_name).set("num_geoms",intersect.num_geoms);
                    });
                    //console.log(self.collection);//right
                });
        },

        getIntersection: function(){
            var self = this;
            var q = self.getGeoJson();
            return $.ajax({
                url: '/v1/api/shapes/intersections/'+ q,
               // url: "http://plenar.io/v1/api/shapes/intersections/{'type':'Feature','properties':{},'geometry':{'type':'Polygon','coordinates':[[[-87.67248630523682,41.86454328565965],[-87.67248630523682,41.872117384500754],[-87.6549768447876,41.872117384500754],[-87.6549768447876,41.86454328565965],[-87.67248630523682,41.86454328565965]]]}}",
                //crossOrigin: true,
                //xhrFields: {withCredentials:true},
                dataType: 'json',
            });
        },
        getGeoJson: function() {
            var self = this;
            return self.query.location_geom__within;
        }
    });

