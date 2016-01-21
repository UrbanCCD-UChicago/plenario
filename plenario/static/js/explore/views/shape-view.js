var app = app || {};

    app.ShapeView = Backbone.View.extend({

        el: '#shapes-view',
        // will go back to use template after fully integrate backbone with the original application
        //template:
        events: {
            'click .shape-detail': 'shapeDetailView'
        },

        initialize: function() {
            var self = this;
            this.query = {};
            this.collection = new app.Shapes();
            this.collection.fetch({reset:true,success:function(){
                if (resp && resp.query.location_geom__within) {
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
            if (resp === undefined || this.getGeoJson() === undefined) {
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
            if (self.query){
                 return self.query.location_geom__within;
            }
        },
        shapeDetailView: function(e){
            this.undelegateEvents();

            //If no query has been made, setting default values
            if (_.isEmpty(this.query)) {
                var start = $('#start-date-filter').val();
                var end = $('#end-date-filter').val();
                start = moment(start);
                if (!start){ start = moment().subtract('days', 90); }
                end = moment(end);
                if(!end){ end = moment(); }
                start = start.startOf('day').format('YYYY/MM/DD');
                end = end.endOf('day').format('YYYY/MM/DD');
                this.query['obs_date__le'] = end;
                this.query['obs_date__ge'] = start;
                this.query['agg'] = $('#time-agg-filter').val();
                this.query['resolution'] = "500";
            }

            var dataset_name = $(e.target).data('shape_dataset_name');
            this.query['shape_dataset_name'] = dataset_name;

            $('#map-view').empty();
            // currently not rendering meta data and have no filter options
            shapeDetailView = new app.ShapeDetailView({model:this.collection.get(dataset_name), query:this.query});
            var route = 'shapeDetail/' + $.param(this.query);
            _gaq.push(['_trackPageview', route]);
            router.navigate(route);
        }
    });

