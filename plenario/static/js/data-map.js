(function(){
    var map;
    var geojson = null;
    var results = null;
    var resp;
    function template_cache(tmpl_name, tmpl_data){
        if ( !template_cache.tmpl_cache ) {
            template_cache.tmpl_cache = {};
        }

        if ( ! template_cache.tmpl_cache[tmpl_name] ) {
            var tmpl_dir = '/static/js/templates';
            var tmpl_url = tmpl_dir + '/' + tmpl_name + '.html';

            var tmpl_string;
            $.ajax({
                url: tmpl_url,
                method: 'GET',
                async: false,
                success: function(data) {
                    tmpl_string = data;
                }
            });

            template_cache.tmpl_cache[tmpl_name] = _.template(tmpl_string);
        }

        return template_cache.tmpl_cache[tmpl_name](tmpl_data);
    }
    var ErrorView = Backbone.View.extend({
        initialize: function(){
            this.render()
        },
        render: function(){
            this.$el.html(template_cache('modalTemplate', this.model));
            this.$el.modal();
            return this;
        }
    });
    var QueryView = Backbone.View.extend({
        initialize: function(){
            this.render()
        },
        render: function(){
            var query = this.attributes.query;
            this.$el.html(template_cache('queryTemplate', {query: query}));
        }
    })

    var DetailView = Backbone.View.extend({
        initialize: function(){
            window.scrollTo(0, 0);
            this.$el.empty()
            this.query = this.attributes.query;
            this.meta = this.attributes.meta;
            this.render()
        },
        render: function(){
            $('#list-view').hide();
            $('#detail-view').show();
            this.$el.html(template_cache('detailTemplate', {query: this.query, meta: this.meta}));
        }
    })

    var ResponseView = Backbone.View.extend({
        events: {
            'click .detail': 'detailView'
        },
        render: function(){
            $('#list-view').show();
            $('#detail-view').hide();
            var self = this;
            this.query = this.attributes.query;
            if (typeof this.explore !== 'undefined'){
                this.explore.remove();
            }
            this.$el.empty();
            this.charts = {};
            this.$el.spin('large');
            this.getResults();
        },
        detailView: function(e){
            var dataset_name = $(e.target).data('dataset_name')
            this.query['dataset_name'] = dataset_name
            var detail_view = new DetailView({el:'#detail-view', attributes: {query: this.query, meta: this.meta[dataset_name]}})
            $('#map-view').empty();
            new GridMapView({el: '#map-view', attributes: {query: this.query, meta: this.meta[dataset_name]}})
            var route = 'detail/' + $.param(this.query)
            router.navigate(route)
        },
        getResults: function(){
            var self = this;
            $.when(this.resultsFetcher(), this.metaFetcher()).then(
                function(resp, meta_resp){
                    self.$el.spin(false);
                    var results = resp[0].objects;
                    var m = meta_resp[0]
                    var objects = []
                    self.meta = {}
                    $.each(m, function(i, obj){
                        self.meta[obj.dataset_name] = obj
                    })
                    $.each(results, function(i, obj){
                        obj['values'] = []
                        $.each(obj.items, function(i, o){
                            obj['values'].push(o.count);
                        });
                        obj['meta'] = self.meta[obj['dataset_name']]
                        objects.push(obj)
                    });
                    self.$el.html(template_cache('datasetTable', {
                        objects: objects,
                        query: self.query
                    }));

                    // Sparklines
                      $(".sparkline").sparkline("html", {
                        chartRangeMin: 0,
                        fillColor: "#ddf2fb",
                        height: "30px",
                        lineColor: "#518fc9",
                        lineWidth: 1,
                        minSpotColor: "#0b810b",
                        maxSpotColor: "#c10202",
                        spotColor: false,
                        spotRadius: 2,
                        width: "290px"
                      });

                    $('#response-datasets').DataTable( {
                        "aaSorting": [ [0,'asc'] ],
                        "aoColumns": [
                            null,
                            { "bSortable": false },
                            { "bSortable": false }
                        ],
                        "paging": false,
                        "searching": false,
                        "info": false
                    } );
                }
            ).fail(function(resp){
                new ErrorView({el: '#errorModal', model: resp});
            });
        },
        resultsFetcher: function(){
            var self = this;
            return $.ajax({
                url: '/api/master/',
                dataType: 'json',
                data: self.query
            });
        },
        metaFetcher: function(){
            return $.ajax({
                url: '/api/',
                dataType: 'json'
            })
        }
    });
    var AboutView = Backbone.View.extend({
        events: {
            'click .detail': 'detailView'
        },
        initialize: function(){
            this.render();
        },
        render: function(){
            $('#list-view').show();
            $('#detail-view').hide();
            this.$el.empty();
            this.$el.spin('large');
            var self = this;
            $.when(this.get_datasets()).then(
                function(resp){
                    self.$el.spin(false);
                    self.$el.html(template_cache('aboutTemplate', {datasets:resp}));
                    var dataObjs = {}
                    console.log(resp);
                    $.each(resp, function(i, obj){
                        dataObjs[obj['dataset_name']] = obj;
                    })
                    self.datasetsObj = dataObjs;

                    $('#available-datasets').DataTable( {
                        "aaSorting": [ [0,'asc'] ],
                        "aoColumns": [
                            null,
                            null,
                            { "bSortable": false }
                        ],
                        "paging": false,
                        "searching": false,
                        "info": false
                    } );
                }
            )
        },
        get_datasets: function(){
            return $.ajax({
                url: '/api/',
                dataType: 'json'
            })
        },
        detailView: function(e){

            var query = {};
            var start = $('#start-date-filter').val();
            var end = $('#end-date-filter').val();
            start = moment(start);
            if (!start){ start = moment().subtract('days', 180); }
            end = moment(end)
            if(!end){ end = moment(); }
            start = start.startOf('day').format('YYYY/MM/DD');
            end = end.endOf('day').format('YYYY/MM/DD');

            query['obs_date__le'] = end;
            query['obs_date__ge'] = start;
            query['agg'] = $('#time-agg-filter').val();

            var dataset_name = $(e.target).data('dataset_name')
            console.log(dataset_name);
            query['dataset_name'] = dataset_name

            new DetailView({el:'#detail-view', attributes: {query: query, meta: this.datasetsObj[dataset_name]}})
            $('#map-view').empty();
            new GridMapView({el: '#map-view', attributes: {query: query, meta: this.datasetsObj[dataset_name]}})
            var route = 'detail/' + $.param(query)
            router.navigate(route)
        }
    });

    var GridMapView = Backbone.View.extend({
        events: {
            'change #spatial-agg-filter': 'changeSpatialAgg'
        },
        initialize: function(){
            this.center = [41.880517,-87.644061];
            this.query = this.attributes.query;
            this.meta = this.attributes.meta;
            this.$el.html(template_cache('gridMapTemplate', {query: this.query, meta: this.meta}));
            var map_options = {
                scrollWheelZoom: false,
                tapTolerance: 30,
                minZoom: 1
            };
            this.map = L.map('map', map_options).setView(this.center, 11);
            L.tileLayer('https://{s}.tiles.mapbox.com/v3/derekeder.hehblhbj/{z}/{x}/{y}.png', {
              attribution: '<a href="http://www.mapbox.com/about/maps/" target="_blank">Terms &amp; Feedback</a>'
            }).addTo(this.map);
            this.resolution = 500;
            this.legend = L.control({position: 'bottomright'});
            this.jenksCutoffs = {}
            var self = this;

            this.legend.onAdd = function (map) {
                var div = L.DomUtil.create('div', 'legend'),
                    grades = self.jenksCutoffs,
                    labels = [],
                    from, to;

                labels.push('<i style="background-color:' + self.getColor(0) + '"></i> 0');
                labels.push('<i style="background-color:' + self.getColor(1) + '"></i> 1 &ndash; ' + grades[2]);
                for (var i = 2; i < grades.length; i++) {
                    from = grades[i] + 1;
                    to = grades[i + 1];
                    labels.push(
                        '<i style="background-color:' + self.getColor(from + 1) + '"></i> ' +
                        from + (to ? '&ndash;' + to : '+'));
                }

                div.innerHTML = '<div><strong>' + self.meta['human_name'] + '</strong><br />' + labels.join('<br />') + '</div>';
                return div;
            };

            this.gridLayer = new L.FeatureGroup();
            this.mapColors = [
                '#eff3ff',
                '#bdd7e7',
                '#6baed6',
                '#3182bd',
                '#08519c'
            ]
            this.render();
        },
        render: function(){
            var self = this;
            this.$el.spin('large');
            $.when(this.getGrid()).then(
                function(resp){
                    self.$el.spin(false);
                    var values = [];
                    $.each(resp['features'], function(i, val){
                        values.push(val['properties']['count']);
                    });
                    try{self.legend.removeFrom(self.map);}catch(e){};
                    self.gridLayer.clearLayers();
                    if (typeof self.gridLayer !== 'undefined'){
                        self.map.removeLayer(self.gridLayer);
                    }
                    self.jenksCutoffs = self.getCutoffs(values);
                    if (values.length > 0){
                        self.gridLayer.addLayer(L.geoJson(resp, {
                            style: function(feature){
                                return {
                                    fillColor: self.getColor(feature.properties.count),
                                    weight: 0.3,
                                    opacity: 1,
                                    color: 'white',
                                    fillOpacity: 0.7
                                }
                            },
                            onEachFeature: self.onEachFeature
                        })).addTo(self.map);
                        self.legend.addTo(self.map);
                        self.map.fitBounds(self.gridLayer.getBounds());
                    }
                }
            )
        },
        changeSpatialAgg: function(e){
            this.resolution = $(e.target).val()
            this.render()
        },
        getGrid: function(){
            var q = this.query;
            q['resolution'] = this.resolution
            q['center'] = this.center
            delete q['agg']
            return $.ajax({
                url: '/api/grid/',
                dataType: 'json',
                data: q
            })
        },
        getCutoffs: function(values){
            var jenks_cutoffs = jenks(values, 4);
            jenks_cutoffs.unshift(0); // set the bottom value to 0
            jenks_cutoffs[1] = 1; // set the second value to 1
            jenks_cutoffs.pop(); // last item is the max value, so dont use it
            return jenks_cutoffs;
        },
        getColor: function(d){
            return  d >  this.jenksCutoffs[4] ? this.mapColors[4] :
                    d >  this.jenksCutoffs[3] ? this.mapColors[3] :
                    d >  this.jenksCutoffs[2] ? this.mapColors[2] :
                    d >= this.jenksCutoffs[1] ? this.mapColors[1] :
                                           this.mapColors[0];
        },
        styleGrid: function(feature){
            var self = this;
            return {
                fillColor: self.getColor(feature.properties.count),
                weight: 0.3,
                opacity: 1,
                color: 'white',
                fillOpacity: 0.7
            }
        },
        onEachFeature: function(feature, layer){
            var content = '<h4>Count: ' + feature.properties.count + '</h4>';
            layer.bindLabel(content);
        }
    })

    var MapView = Backbone.View.extend({
        events: {
            'click #submit-query': 'submitForm',
            'click #reset': 'resetForm'
        },
        initialize: function(){
            var then = moment().subtract('d', 180).format('MM/DD/YYYY');
            var now = moment().format('MM/DD/YYYY');
            this.$el.html(template_cache('mapTemplate', {end: now, start: then}));

            // initialize the Leaflet map
            var map_options = {
                scrollWheelZoom: false,
                tapTolerance: 30,
                minZoom: 1
            };
            this.map = L.map('map', map_options).setView([41.880517,-87.644061], 11);
            L.tileLayer('https://{s}.tiles.mapbox.com/v3/derekeder.hehblhbj/{z}/{x}/{y}.png', {
              attribution: '<a href="http://www.mapbox.com/about/maps/" target="_blank">Terms &amp; Feedback</a>'
            }).addTo(this.map);
            this.map.drawnItems = new L.FeatureGroup();
            this.map.addLayer(this.map.drawnItems);
            this.render();
        },
        render: function(){
            var self = this;
            var drawControl = new L.Control.Draw({
                edit: {
                    featureGroup: self.map.drawnItems
                },
                draw: {
                    circle: false,
                    marker: false
                }
            });
            this.map.addControl(drawControl);
            this.map.on('draw:created', this.drawCreate);
            this.map.on('draw:drawstart', this.drawDelete);
            this.map.on('draw:edited', this.drawEdit);
            this.map.on('draw:deleted', this.drawDelete);
            $('.date-filter').datepicker({
                dayNamesMin: ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'],
                prevText: '',
                nextText: ''
            });
            if (typeof this.attributes.dataLayer !== 'undefined'){
                this.map.drawnItems.addLayer(
                    L.geoJson(this.attributes.dataLayer, {
                        color: "#f06eaa",
                        fillColor: "#f06eaa",
                        weight: 4
                }));
                //this.map.fitBounds(this.map.drawnItems.getBounds());
            }

            $("#dismiss-intro").click(function(){
                $('#collapse-intro').collapse('hide');
            });
        },
        resetForm: function(e){
            window.location = "/explore";
        },
        drawCreate: function(e){
            this.drawnItems.clearLayers();
            this.drawnItems.addLayer(e.layer);
            this.dataLayer = e.layer.toGeoJSON();
        },
        drawDelete: function(e){
            this.drawnItems.clearLayers();
        },
        drawEdit: function(e){
            var layers = e.layers;
            this.drawnItems.clearLayers();
            var self = this;
            layers.eachLayer(function(layer){
                self.dataLayer = layer.toGeoJSON();
                self.drawnItems.addLayer(layer);
            });
        },
        submitForm: function(e){
            var message = null;
            var query = {};
            var start = $('#start-date-filter').val();
            var end = $('#end-date-filter').val();
            start = moment(start);
            if (!start){
                start = moment().subtract('days', 180);
            }
            end = moment(end)
            if(!end){
                end = moment();
            }
            var valid = true;
            if (start.isValid() && end.isValid()){
                start = start.startOf('day').format('YYYY/MM/DD');
                end = end.endOf('day').format('YYYY/MM/DD');
            } else {
                valid = false;
                message = 'Your dates are not entered correctly';
            }
            query['obs_date__le'] = end;
            query['obs_date__ge'] = start;
            if (this.map.dataLayer){
                query['location_geom__within'] = JSON.stringify(this.map.dataLayer);
                this.map.fitBounds(this.map.drawnItems.getBounds());
            }
            query['agg'] = $('#time-agg-filter').val();
            if(valid){
                var resp = new ResponseView({el: '#list-view'})
                resp.attributes = {query: query};
                resp.render();
                var route = "aggregate/" + $.param(query);
                router.navigate(route);
            } else {
                $('#list-view').spin(false);
                var error = {
                    header: 'Woops!',
                    body: message,
                }
                new ErrorView({el: '#errorModal', model: resp});
            }
        }
    });

    var AppRouter = Backbone.Router.extend({
        routes: {
            "": "defaultRoute",
            "aggregate/:query": "aggregate",
            "detail/:query": "detail"
        },
        defaultRoute: function(){
            var about = new AboutView({el: '#list-view'});
            var map = new MapView({el: '#map-view', attributes: {}})
        },
        aggregate: function(query){
            var q = parseParams(query);
            var resp = new ResponseView({el: '#list-view', attributes: {query: q}});
            resp.render();
            var attrs = {
                resp: resp
            }
            if (typeof q['location_geom__within'] !== 'undefined'){
                attrs['dataLayer'] = $.parseJSON(q['location_geom__within']);
            }
            var map = new MapView({el: '#map-view', attributes: attrs});
        },
        detail: function(query){
            var q = parseParams(query);
            var dataset = q['dataset_name']
            $.when($.getJSON('/api/', {dataset_name: dataset})).then(
                function(resp){
                    new DetailView({el: '#detail-view', attributes: {query: q, meta: resp[0]}});
                    new GridMapView({el: '#map-view', attributes: {query: q, meta: resp[0]}})
                }
            )
        }
    });

    function resize_page(){
        $('.half-height').height((window.innerHeight  / 2) - 40);
    }

    function parseParams(query){
        var re = /([^&=]+)=?([^&]*)/g;
        var decodeRE = /\+/g;  // Regex for replacing addition symbol with a space
        var decode = function (str) {return decodeURIComponent( str.replace(decodeRE, " ") );};
        var params = {}, e;
        while ( e = re.exec(query) ) {
            var k = decode( e[1] ), v = decode( e[2] );
            if (k.substring(k.length - 2) === '[]') {
                k = k.substring(0, k.length - 2);
                (params[k] || (params[k] = [])).push(v);
            }
            else params[k] = v;
        }
        return params;
    }

    var router = new AppRouter();
    Backbone.history.start();

})()
