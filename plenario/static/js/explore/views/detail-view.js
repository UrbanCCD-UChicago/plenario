var DetailView = Backbone.View.extend({
    events: {
        'click #add-filter': 'addFilter',
        'click #submit-detail-query': 'submitForm'
    },
    initialize: function(){
        this.center = [41.880517,-87.644061];
        this.query = this.attributes.query;
        this.meta = this.attributes.meta;

        var start = moment().subtract('d', 180).format('MM/DD/YYYY');
        var end = moment().format('MM/DD/YYYY');

        if (this.query) {
            start = moment(this.query.obs_date__ge).format('MM/DD/YYYY');
            end = moment(this.query.obs_date__le).format('MM/DD/YYYY');
        }

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
        L.tileLayer('https://{s}.tiles.mapbox.com/v3/derekeder.hehblhbj/{z}/{x}/{y}.png', {
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

        // grab the field options before we move on to the render step
        self.field_options = {}
        $.when($.get('/api/fields/' + self.query['dataset_name'])).then(function(field_options){
            self.field_options = field_options;
            self.render();
        });
    },
    render: function(){
        var self = this;
        $('#detail-view').hide();
        $('#list-view').hide();

        $('#download-geojson').attr('href','/api/grid/?' + $.param(self.getQuery()))
        $('.date-filter').datepicker({
            dayNamesMin: ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'],
            prevText: '',
            nextText: ''
        });

        $('#spatial-agg-filter').val(this.query['resolution']);

        $("#detail-chart").spin('large');
        $.when(this.getTimeSeries()).then( function(resp){
            $("#detail-chart").spin(false);
            var chart_vals = [];
            var record_count = 0;
            $.each(resp['objects'], function(i, o){
                chart_vals.push([moment(o.datetime + "+0000").valueOf(),o.count]);
                record_count += o.count;
            });
            $("#record-count").html(record_count + " records")
            ChartHelper.sparkline("detail-chart", "day", chart_vals);
        });

        // populate filters from query
        var params_to_exclude = ['obs_date__ge', 'obs_date__le', 'dataset_name', 'resolution' , 'center', 'buffer'];

        // grab a list of dataset fields from the /api/fields/ endpoint
        // create a new empty filter
        new FilterView({el: '#filter_builder', attributes: {filter_dict: {"id" : 0, "field" : "", "value" : "", "operator" : "", "removable": false }, field_options: self.field_options}})

        // render filters based on self.query
        var i = 1;
        $.each(self.query, function(key, val){
            //exclude reserved query parameters
            if ($.inArray(key, params_to_exclude) == -1) {
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
        //query['center'] = this.query['center'];
        //query['buffer'] = this.query['buffer'];

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
            new DetailView({el: '#map-view', attributes: {query: query, meta: this.meta}})
            var route = 'detail/' + $.param(query)
            router.navigate(route)
        } else {
            $('#map-view').spin(false);
            var error = {
                header: 'Woops!',
                body: message,
            }
            new ErrorView({el: '#errorModal', model: resp});
        }
    },

    getQuery: function(){
        var q = this.query;
        delete q['agg']
        return q
    },
    getTimeSeries: function(){
        var q = this.points_query;
        return $.ajax({
            url: '/api/detail-aggregate/',
            dataType: 'json',
            data: q
        })
    },
    getGrid: function(){
        var q = this.getQuery()
        return $.ajax({
            url: '/api/grid/',
            dataType: 'json',
            data: q
        })
    },
    getFields: function(){
        var q = this.getQuery()
        return $.ajax({
            url: ('/api/fields/' + q['dataset_name'])
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
});