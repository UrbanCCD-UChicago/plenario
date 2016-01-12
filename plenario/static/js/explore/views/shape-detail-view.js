var app = app || {};

    app.ShapeDetailView = Backbone.View.extend({
        el: '#map-view',

        events: {
            'click #back-to-explorer-shape': 'backToExplorerShape'
        },

    initialize: function(options){
        this.query = options.query;
        this.shape_query = options.shape_query;
        this.$el.html(template_cache('shapeDetailTemplate', {shape_query: this.shape_query, shape: this.model.toJSON()}));

        var map_options = {
            scrollWheelZoom: false,
            tapTolerance: 30,
            minZoom: 1
        };

        this.map = L.map('map', map_options).setView([41.880517,-87.644061], 11);
        L.tileLayer('https://{s}.tiles.mapbox.com/v3/datamade.hn83a654/{z}/{x}/{y}.png', {
          attribution: '<a href="http://www.mapbox.com/about/maps/" target="_blank">Terms &amp; Feedback</a>'
        }).addTo(this.map);
        this.legend = L.control({position: 'bottomright'});
        this.polygonResponse();
        this.respLayer = new L.featureGroup();
        this.render();
    },

    render: function(){
        window.scrollTo(0, 0);
        $('#detail-view').hide();
        $('#list-view').hide();
        $('#shapes-view').hide();
        $("#map").spin('large');
        $("#detail-chart").spin('large');
        if (this.shape_query['location_geom__within']) {
            this.respLayer.addLayer(L.geoJson(jQuery.parseJSON(this.shape_query['location_geom__within']),
                {color: "#f06eaa", fillColor: "#f06eaa", weight:4 }));
            this.respLayer.addTo(this.map);
            this.map.fitBounds(this.respLayer.getBounds());
        }
    },

    polygonResponse: function () {
       var self = this;
       $.when($.getJSON('http://plenar.io/v1/api/shapes/'+ this.shape_query.shape_dataset_name + '?data_type=json')).then(
            function(resp){
                $('#map').spin(false);
                var data = resp;
                var geojsonLayer = L.geoJson(data, {});
                geojsonLayer.addTo(self.map);
            });
    },

    backToExplorerShape: function(e){
        //console.log("going back to aggregate from shape detail view");

        e.preventDefault();
        this.undelegateEvents();
        delete this.query['resolution'];
        delete this.query['dataset_name'];

        var attrs = { resp: resp };

        if (typeof this.shape_query['location_geom__within'] !== 'undefined'){
            attrs['dataLayer'] = $.parseJSON(this.shape_query['location_geom__within']);
        }

        if (resp) { resp.undelegateEvents(); }
        resp = new ResponseView({el: '#list-view', attributes: {query: this.query}});

        if (map) { map.undelegateEvents(); }
        map = new MapView({el: '#map-view', attributes: attrs});

        shapeView = new app.ShapeView({el:'#shapes-view'});

        var route = "aggregate/" + $.param(this.shape_query);
        _gaq.push(['_trackPageview', route]);
        router.navigate(route);
    }
});
