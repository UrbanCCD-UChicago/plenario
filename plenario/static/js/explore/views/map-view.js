var MapView = Backbone.View.extend({
    events: {
        'click #submit-query': 'submitForm',
        'click #reset': 'resetForm'
    },
    initialize: function(){
        var start = moment().subtract('d', 90).format('MM/DD/YYYY');
        var end = moment().format('MM/DD/YYYY');

        if (this.attributes.resp && this.attributes.resp.query)
        {
            start = moment(this.attributes.resp.query.obs_date__ge).format('MM/DD/YYYY');
            end = moment(this.attributes.resp.query.obs_date__le).format('MM/DD/YYYY');
        }

        this.$el.html(template_cache('mapTemplate', {end: end, start: start}));

        if (this.attributes.resp && this.attributes.resp.query.agg)
            $('#time-agg-filter').val(this.attributes.resp.query.agg)

        // initialize the Leaflet map
        var map_options = {
            scrollWheelZoom: false,
            tapTolerance: 30,
            minZoom: 1
        };
        this.map = L.map('map', map_options).setView([41.880517,-87.644061], 11);
        L.tileLayer('https://{s}.tiles.mapbox.com/v3/datamade.hn83a654/{z}/{x}/{y}.png', {
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

        var geojson = L.geoJson(this.attributes.dataLayer, {
                      color: "#f06eaa",
                      fillColor: "#f06eaa",
                      weight: 4
                    });
        if (typeof this.attributes.dataLayer !== 'undefined'){
            this.map.drawnItems.addLayer(geojson);

            this.map.whenReady(function () {
                window.setTimeout(function () {
                    this.map.fitBounds(geojson.getBounds());
                }.bind(this), 200);
            }, this);
        }

        $("#dismiss-intro").click(function(e){
            e.preventDefault();
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
        // console.log('map-view submit')
        var message = null;
        var query = {};
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
            message = 'Your dates are not entered correctly. Please enter them in the format month/day/year.';
        }
        query['obs_date__le'] = end;
        query['obs_date__ge'] = start;
        if (this.map.dataLayer){
            query['location_geom__within'] = JSON.stringify(this.map.dataLayer);
            this.map.fitBounds(this.map.drawnItems.getBounds());
        }
        else if (this.attributes.resp && this.attributes.resp.query.location_geom__within) {
            query['location_geom__within'] = this.attributes.resp.query.location_geom__within
        }
        else {
            valid = false;
            message = 'You must draw a shape on the map to continue your search.';
        }
        query['agg'] = $('#time-agg-filter').val();

        if(valid){
            if (resp) { resp.undelegateEvents(); }
            resp = new ResponseView({el: '#list-view', attributes: {query: query}})
            var route = "aggregate/" + $.param(query);
            _gaq.push(['_trackPageview', route]);
            router.navigate(route);
        } else {
            $('#list-view').spin(false);
            var error = {
                header: 'Woops!',
                body: message,
            }
            new ErrorView({el: '#errorModal', model: error});
        }
    }
});
