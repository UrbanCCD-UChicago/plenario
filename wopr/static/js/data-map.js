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
            var tmpl_url = tmpl_dir + '/' + tmpl_name + '.html?2';

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
    var ChartView = Backbone.View.extend({
        events: {
            'click .data-download': 'fetchDownload',
        },
        render: function(){
            this.$el.html(template_cache('chartTemplate', this.model));
            return this;
        },
        addData: function(data){
            var el = this.model['el'];
            var name = this.model['objects']['dataset_name'];
            var agg = this.model.query.agg;
            var iteration = this.model.iteration;
            ChartHelper.create(el, name, 'City of Chicago', agg, data, iteration);
        },
        fetchDownload: function(e){
            this.model.query['dataset_name'] = $(e.target).attr('id').split('-')[0];
            this.model.query['datatype'] = $(e.target).attr('id').split('-')[1];
            if ($(e.target).parent().parent().hasClass('detail')){
                var path = 'detail';
            } else {
                var path = 'master';
            }
            var url = '/api/' + path + '/?' + $.param(this.model.query);
            window.open(url, '_blank');
        }
    });
    var ExploreView = Backbone.View.extend({
        events: {
            'click .refine': 'refineQuery'
        },
        render: function(){
            var dataset = this.attributes.base_query['dataset_name']
            var self = this;
            $.when(this.getFields(dataset)).then(
                function(fields){
                    self.$el.html(template_cache('exploreForm', {
                        fields: fields.objects
                    }));
                    self.queryView = new QueryView({
                        el: '#query',
                        attributes: {
                            query: self.attributes.base_query
                        }
                    });
                    self.delegateEvents();
                }
            )
            return this;
        },
        refineQuery: function(e){
            var refined = this.$el.find('textarea').val();
            var refined_query = parseParams(refined);
            var query = $.extend(refined_query, this.attributes.base_query);
            var dataset_name = this.attributes.base_query['dataset_name'];
            this.attributes.parent.charts[dataset_name].undelegateEvents();
            this.attributes.parent.charts[dataset_name].$el.empty();
            if (typeof this.refine !== 'undefined'){
                this.refine.undelegateEvents();
                this.refine.$el.empty();
            }
            var self = this;
            this.$el.spin('large');
            $.when(this.getData(query)).then(
                function(data){
                    self.$el.spin(false);
                    query.dataset_name = self.attributes.base_query['dataset_name'];
                    self.refine = new RefineView({
                        el: '#refine',
                        attributes: {
                            data: data,
                            query: query
                        }
                    });
                    self.refine.render();
                }
            ).fail(function(resp){
                new ErrorView({el: '#errorModal', model: resp});
            });
        },
        getFields: function(dataset){
            return $.ajax({
                url: '/api/fields/' + dataset + '/',
                dataType: 'json',
            });
        },
        getData: function(query){
            return $.ajax({
                url: '/api/detail-aggregate/',
                dataType: 'json',
                data: query
            });
        }
    });
    var RefineView = Backbone.View.extend({
        render: function(){
            var data = this.attributes.data;
            var el = this.attributes.query.dataset_name;
            var item = {
                el: el,
                objects: data.objects[0],
                query: this.attributes.query,
                iteration: 0,
                detail: true
            }
            var chart = new ChartView({
                model: item
            });
            this.$el.html(chart.render().el);
            var objs = [];
            $.each(data.objects, function(i,obj){
                $.each(obj.items, function(j, o){
                    objs.push([moment(o.group).unix()*1000, o.count]);
                })
            });
            this.queryView = new QueryView({
                el: '#query',
                attributes: {
                    query: this.attributes.query
                }
            });
            chart.addData(objs);
            this.chart = chart;
        }
    });
    var ResponseView = Backbone.View.extend({
        events: {
            'click .explore': 'exploreDataset'
        },
        initialize: function(){
            //this.explore = new ExploreView({el: '#explore'});
        },
        render: function(){
            var self = this;
            this.query = this.attributes.query;
            if (typeof this.explore !== 'undefined'){
                this.explore.remove();
            }
            this.$el.empty();
            this.charts = {};
            this.$el.spin('large');
            //if (!this.attributes.explore){
            this.getResults();
            //}
        },
        exploreDataset: function(e){
            var self = this;
            this.query['dataset_name'] = $(e.target).attr('id').split('-')[0];
            this.query['datatype'] = 'json';
            $.each(this.charts, function(key,chart){
                if (key != self.query.dataset_name){
                    chart.remove();
                }
            });
            this.explore.attributes = {
                base_query: this.query,
                parent: this
            }
            this.$el.after(this.explore.render().el);
            var route = "detail/" + $.param(this.query);
            router.navigate(route);
        },
        getResults: function(){
            var self = this;
            $.when(this.resultsFetcher()).then(function(resp){
                self.$el.spin(false);
                results = resp.objects;
                var objects = []
                $.each(results, function(i, obj){
                    obj['values'] = []
                    $.each(obj.items, function(i, o){
                        obj['values'].push(o.count);
                    });
                    obj['meta'] = self.about.datasetsObj[obj['dataset_name']]
                    objects.push(obj)
                });
                self.$el.html(template_cache('datasetTable', {
                    objects: objects,
                    query: self.query
                }));
                $.each(objects, function(i, obj){
                    $('#' + obj.meta.dataset_name + '-sparkline').sparkline(
                        obj.values, {
                        width: '200px',
                        tooltipClassname: 'sparkline-tooltip'
                    });
                })
                $('#about').hide();
            }).fail(function(resp){
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
        }
    });
    var AboutView = Backbone.View.extend({
        initialize: function(){
            this.render();
        },
        render: function(){
            this.$el.empty();
            this.$el.spin('large');
            var self = this;
            $.when(this.get_datasets()).then(
                function(resp){
                    self.$el.spin(false);
                    self.$el.html(template_cache('aboutTemplate', {datasets:resp}));
                    var dataObjs = {}
                    $.each(resp, function(i, obj){
                        dataObjs[obj['dataset_name']] = obj;
                    })
                    self.datasetsObj = dataObjs;
                }
            )
        },
        get_datasets: function(){
            return $.ajax({
                url: '/api/',
                dataType: 'json'
            })
        }
    });

    var MapView = Backbone.View.extend({
        events: {
            'click #submit-query': 'submitForm',
            'click #reset': 'resetForm'
        },
        initialize: function(){
            this.resp = this.attributes.resp;
            this.resp.about = this.attributes.about;
            var then = moment().subtract('d', 180).format('MM/DD/YYYY');
            var now = moment().format('MM/DD/YYYY');
            this.$el.html(template_cache('mapTemplate', {end: now, start: then}));
            this.map = L.map('map').setView([41.880517,-87.644061], 11);
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
            }
        },
        resetForm: function(e){
            window.location.reload();
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
            }
            query['agg'] = $('#time-agg-filter').val();
            if(valid){
                $('#refine').empty();
                this.resp.undelegateEvents();
                this.resp.delegateEvents();
                this.resp.attributes = {query: query};
                this.resp.render();
                var route = "aggregate/" + $.param(query);
                router.navigate(route);
            } else {
                $('#response').spin(false);
                var error = {
                    header: 'Woops!',
                    body: message,
                }
                var errortpl = new EJS({url: 'js/templates/modalTemplate.ejs'})
                $('#errorModal').html(errortpl.render(error));
                $('#errorModal').modal();
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
            var resp = new ResponseView({el: '#response'});
            var about = new AboutView({el: '#about'});
            var map = new MapView({el: '#map-view', attributes: {resp: resp, about: about}})
        },
        aggregate: function(query){
            var q = parseParams(query);
            var resp = new ResponseView({el: '#response', attributes: {query: q}});
            resp.render();
            var attrs = {resp:resp}
            if (typeof q['location_geom__within'] !== 'undefined'){
                attrs['dataLayer'] = $.parseJSON(q['location_geom__within']);
            }
            var map = new MapView({el: '#map-view', attributes: attrs});
        },
        detail: function(query){
            var q = parseParams(query);
            var resp = new ResponseView({el: '#response', attributes: {query: q}});
            resp.render()
            resp.explore.attributes = {
                base_query: q,
                parent: resp
            }
            resp.$el.after(resp.explore.render().el);
            var map = new MapView({el: '#map-view', attributes: {resp: resp}});
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
