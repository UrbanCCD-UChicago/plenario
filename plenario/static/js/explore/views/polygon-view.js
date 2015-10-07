var PolygonView = Backbone.View.extend({
    initialize: function() {
        var self = this
        console.log('Constructed PolygonView!')
        console.log(this.attributes.polygonName)
        var name = this.attributes.polygonName
        $.when($.ajax({
            url: '/v1/api/polygons/' + name,
            dataType: 'json',
        })).then(
            function(chicagoGeoJson) {
                //console.log(chicagoGeoJson)

                // initialize the Leaflet map
                var map_options = {
                    scrollWheelZoom: false,
                    tapTolerance: 30,
                    minZoom: 1
                };
                self.$el.html(template_cache('mapTemplate', {end: moment(), start: moment()}));
                // Center in Chicago, beacuse we know that's what we're looking at.
                self.map = L.map('map', map_options).setView([41.880517,-87.644061], 11);
                // I'll need to ask what's going on with mapbox
                L.tileLayer('https://{s}.tiles.mapbox.com/v3/datamade.hn83a654/{z}/{x}/{y}.png', {
                  attribution: '<a href="http://www.mapbox.com/about/maps/" target="_blank">Terms &amp; Feedback</a>'
                }).addTo(self.map);
                L.geoJson(chicagoGeoJson).addTo(self.map);
            }
        )

    },
});