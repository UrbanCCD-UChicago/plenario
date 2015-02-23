var DetailView = Backbone.View.extend({
    events: {
        'click #add-filter': 'addFilter',
        'click #submit-detail-query': 'submitForm',
        'click #back-to-explorer': 'backToExplorer'
    },
    initialize: function(){
        this.center = [41.880517,-87.644061];
        this.query = this.attributes.query;
        this.meta = this.attributes.meta;
        this.filters = this.attributes.filters;

        // console.log('detail-view init')
        // console.log(this.query)

        var start = moment().subtract('d', 90).format('MM/DD/YYYY');
        var end = moment().format('MM/DD/YYYY');

        if (this.query) {
            start = moment(this.query.obs_date__ge).format('MM/DD/YYYY');
            end = moment(this.query.obs_date__le).format('MM/DD/YYYY');
        }

        if (typeof this.query['agg'] == 'undefined')
            this.query['agg'] = "week";

        if (typeof this.query['resolution'] == 'undefined')
            this.query['resolution'] = "500";

        this.points_query = $.extend(true, {}, this.query);
        delete this.points_query['resolution'];
        delete this.points_query['center'];
        this.$el.html(template_cache('detailTemplate', {query: this.query, points_query: this.points_query, meta: this.meta, start: start, end: end}));

        var map_options = {
            scrollWheelZoom: false,
            tapTolerance: 30,
            minZoom: 1
        };
        this.map = L.map('map', map_options).setView(this.center, 11);
        L.tileLayer('https://{s}.tiles.mapbox.com/v3/datamade.hn83a654/{z}/{x}/{y}.png', {
          attribution: '<a href="http://www.mapbox.com/about/maps/" target="_blank">Terms &amp; Feedback</a>'
        }).addTo(this.map);
        this.legend = L.control({position: 'bottomright'});
        this.jenksCutoffs = {}
        var self = this;

        this.legend.onAdd = function (map) {
            var div = L.DomUtil.create('div', 'legend'),
                grades = self.jenksCutoffs,
                labels = [],
                from, to;

            labels.push('<i style="background-color:' + self.getColor(0) + '"></i> 0');
            if (grades[2] == 1)
                labels.push('<i style="background-color:' + self.getColor(1) + '"></i> 1');
            else
                labels.push('<i style="background-color:' + self.getColor(1) + '"></i> 1 &ndash; ' + grades[2]);

            for (var i = 2; i < grades.length; i++) {
                from = grades[i] + 1;
                to = grades[i + 1];

                if (from == to) {
                    labels.push(
                        '<i style="background-color:' + self.getColor(from + 1) + '"></i> ' +
                        from);
                }
                else {
                    labels.push(
                        '<i style="background-color:' + self.getColor(from + 1) + '"></i> ' +
                        from + (to ? '&ndash;' + to : '+'));
                }
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

        self.render();
    },
    render: function(){
        var self = this;
        window.scrollTo(0, 0);
        $('#detail-view').hide();
        $('#list-view').hide();

        $('.download-map-grid').attr('href','/v1/api/grid/?' + $.param(self.getQuery()))
        $('.date-filter').datepicker({
            dayNamesMin: ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'],
            prevText: '',
            nextText: ''
        });

        $('#time-agg-filter').val(this.query['agg']);
        $('#spatial-agg-filter').val(this.query['resolution']);

        filters = {};
        // grab the field options before we render the filters
        self.field_options = {}
        $.when($.get('/v1/api/fields/' + self.query['dataset_name'])).then(function(field_options){
            self.field_options = field_options;

            // populate filters from query
            var params_to_exclude = ['location_geom__within', 'obs_date__ge', 'obs_date__le', 'dataset_name', 'resolution' , 'center', 'buffer', 'agg'];

            // grab a list of dataset fields from the /v1/api/fields/ endpoint
            // create a new empty filter
            new FilterView({el: '#filter_builder', attributes: {filter_dict: {"id" : 0, "field" : "", "value" : "", "operator" : "", "removable": false }, field_options: self.field_options}})

            // render filters based on self.query
            var i = 1;
            $.each(self.query, function(key, val){
                //exclude reserved query parameters
                if ($.inArray(key, params_to_exclude) == -1) {
                    filters[key] = val;
                    // create a dict for each field
                    var field_and_operator = key.split("__");
                    var field = "";
                    var operator = "";
                    if (field_and_operator.length < 2) {
                        field = field_and_operator[0];
                        operator = "";
                    } else {
                        field = field_and_operator[0];
                        operator = field_and_operator[1];
                    }
                    var filter_dict = {"id" : i, "field" : field, "value" : val, "operator" : operator, "removable": true };
                    // console.log(filter_dict);
                    new FilterView({el: '#filter_builder', attributes: {filter_dict: filter_dict, field_options: self.field_options}})

                    i += 1;
                }
            });
        });
        this.filters = filters;

        $("#map").spin('large');
        $.when(this.getGrid()).then(
            function(resp){
                $("#map").spin(false);
                var values = [];
                $.each(resp['features'], function(i, val){
                    values.push(val['properties']['count']);
                });
                try{self.legend.removeFrom(self.map);}catch(e){};
                self.gridLayer.clearLayers();
                if (typeof self.gridLayer !== 'undefined'){
                    self.map.removeLayer(self.gridLayer);
                }
                //check to see if we get any data back
                if (values.length > 0) {
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
            }
        )

        $("#detail-chart").spin('large');
        $.when(this.getTimeSeries()).then( function(resp){
            $("#detail-chart").spin(false);
            var chart_vals = [];
            var record_count = 0;
            $.each(resp['objects'], function(i, o){
                chart_vals.push([moment(o.datetime + "+0000").valueOf(),o.count]);
                record_count += o.count;
            });
            $("#record-count").html(addCommas(record_count) + " records")
            ChartHelper.sparkline("detail-chart", "day", chart_vals);
        });
    },

    addFilter: function(e){
        var filter_ids = []
        $(".filter_row").each(function (key, val) {
            filter_ids.push(parseInt($(val).attr("data-id")));
        });
        new FilterView({el: '#filter_builder', attributes: {filter_dict: {"id" : (Math.max.apply(null, filter_ids) + 1), "field" : "", "value" : "", "operator" : "", "removable": true }, field_options: this.field_options}});
    },

    submitForm: function(e){
        var message = null;
        var query = {};
        query['dataset_name'] = this.query['dataset_name'];
        if (this.query['location_geom__within'])
            query['location_geom__within'] = this.query['location_geom__within'];
        //query['center'] = this.query['center'];
        //query['buffer'] = this.query['buffer'];

        var start = $('#start-date-filter').val();
        var end = $('#end-date-filter').val();

        start = moment(start);
        if (!start){
            start = moment().subtract('days', 90);
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
        query['agg'] = $('#time-agg-filter').val();
        query['resolution'] = $('#spatial-agg-filter').val();

        // update query from filters
        $(".filter_row").each(function (key, val) {

            val = $(val);
            // console.log(val)
            var field = val.find("[id^=field]").val();
            var operator = val.find("[id^=operator]").val();
            var value = val.find("[id^=value]").val();
            // console.log(field)
            if (value) {
                if (operator != "") operator = "__" + operator;
                query[field + operator] = value;
            }
        });

        this.query = query;
        if(valid){
            this.undelegateEvents();
            // console.log(this.query)
            new DetailView({el: '#map-view', attributes: {query: query, meta: this.meta}})
            var route = 'detail/' + $.param(query)
            _gaq.push(['_trackPageview', route]);
            router.navigate(route)
        } else {
            $('#map-view').spin(false);
            var error = {
                header: 'Woops!',
                body: message,
            }
            new ErrorView({el: '#errorModal', model: error});
        }
    },

    backToExplorer: function(e){
        e.preventDefault();
        this.undelegateEvents();
        points_query = this.points_query;

        // delete filters and dataset name from query
        delete points_query['dataset_name'];

        $.each(self.filters, function(key, val){
            delete points_query[key];
        });

        if (resp) { resp.undelegateEvents(); }
        resp = new ResponseView({el: '#list-view', attributes: {query: points_query}});
        var attrs = { resp: resp }
        if (typeof points_query['location_geom__within'] !== 'undefined'){
            attrs['dataLayer'] = $.parseJSON(points_query['location_geom__within']);
        }

        if (map) { map.undelegateEvents(); }
        map = new MapView({el: '#map-view', attributes: attrs});

        var route = "aggregate/" + $.param(points_query);
        _gaq.push(['_trackPageview', route]);
        router.navigate(route);
    },

    getQuery: function(){
        var q = this.query;
        return q
    },
    getTimeSeries: function(){
        var q = this.points_query;
        return $.ajax({
            url: '/v1/api/detail-aggregate/',
            dataType: 'json',
            data: q
        })
    },
    getGrid: function(){
        var q = this.getQuery()
        return $.ajax({
            url: '/v1/api/grid/',
            dataType: 'json',
            data: q
        })
    },
    getFields: function(){
        var q = this.getQuery()
        return $.ajax({
            url: ('/v1/api/fields/' + q['dataset_name'])
        })
    },
    getCutoffs: function(values){

        if (Math.max.apply(null, values) < 5)
            jenks_cutoffs = [0,1,2,3,4]
        else {
            var jenks_cutoffs = jenks(values, 4);
            jenks_cutoffs.unshift(0); // set the bottom value to 0
            jenks_cutoffs[1] = 1; // set the second value to 1
            jenks_cutoffs.pop(); // last item is the max value, so dont use it
        }
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
});
