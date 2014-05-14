$(window).resize(function () {
    var h = $(window).height(),
      offsetTop = 105; // Calculate the top offset

    $('#map').css('height', (h - offsetTop));
}).resize();

(function(){
    var grid_layer;
    var jenks_cutoffs;
    var map = L.map('map').fitBounds([[41.644286009999995, -87.94010087999999], [42.023134979999995, -87.52366115999999]]);
    L.tileLayer('https://{s}.tiles.mapbox.com/v3/datamade.hn83a654/{z}/{x}/{y}.png', {
        attribution: '<a href="http://www.mapbox.com/about/maps/" target="_blank">Terms &amp; Feedback</a>'
    }).addTo(map);
    var grid_data = {
        year: 2013,
        dataset: 'chicago_crimes_all',
        human_name: 'Crimes - 2001 to present',
        resolution: 1000, //in meters
        obs_from: '2001-01-01',
        obs_to: moment().subtract('days', 7).format('YYYY-MM-DD')
    }
    var legend = L.control({position: 'bottomright'});
    legend.onAdd = function(map){
        var div = L.DomUtil.create('div', 'legend')
        var labels = [];
        var from;
        var to;
        $.each(jenks_cutoffs, function(i, grade){
            from = grade
            to = jenks_cutoffs[i + 1];
            labels.push('<i style="background:' + getColor(from) + '"></i>' +
                       from + (to ? '&ndash;' + to : '+'));
        });
        div.innerHTML = '<div><strong>' + grid_data['human_name'] + '</strong><br />' + labels.join('<br />') + '</div>';
        return div
    }
    var map_colors = [
        '#deebf7',
        '#c6dbef',
        '#9ecae1',
        '#6baed6',
        '#4292c6',
        '#2171b5',
        '#084594'
    ]
    $('#dataset-picker').on('change', function(e){
        var name = $(this).val();
        grid_data['dataset'] = name;
        grid_data['human_name'] = $('#dataset-picker').find(':selected').first().text().trim();
        $('.yearpicker').hide();
        $('#' + name + '-yearpicker').show();
        loadLayer(grid_data);
    });
    $('.yearpicker').on('change', function(e){
        grid_data['year'] = parseInt($(this).val());
        loadLayer(grid_data);
    })
    $('#resolution-picker').on('change', function(e){
        grid_data['resolution'] = parseFloat($(this).val());
        loadLayer(grid_data);
    })
    loadLayer(grid_data);
    function loadLayer(grid_data){
        $('#map').spin('large');
        grid_data['center'] = [map.getCenter().lat, map.getCenter().lng]
        var url ='/api/grid/'
        $.when(getGrid(url, grid_data)).then(
            function(grid){
                $('#map').spin(false);
                var values = [];
                $.each(grid['features'], function(i, val){
                    values.push(val['properties']['count']);
                });
                try{legend.removeFrom(map);}catch(e){};
                if (typeof grid_layer !== 'undefined'){
                    map.removeLayer(grid_layer);
                }
                if (values.length > 0){
                    jenks_cutoffs = jenks(values, 6);
                    jenks_cutoffs[0] = 0;
                    jenks_cutoffs.pop();
                    grid_layer = L.geoJson(grid, {
                        style: styleGrid,
                        onEachFeature: function(feature, layer){
                            var content = '<h4>Count: ' + feature.properties.count + '</h4>';
                            layer.bindLabel(content);
                        }
                    }).addTo(map);
                    legend.addTo(map);
                }
                getFieldDefs(grid_data['dataset']);
            }
        );
    }

    function getColor(d){
        return d >= jenks_cutoffs[5] ? map_colors[6] :
               d >= jenks_cutoffs[4] ? map_colors[5] :
               d >= jenks_cutoffs[3] ? map_colors[4] :
               d >= jenks_cutoffs[2] ? map_colors[3] :
               d >= jenks_cutoffs[1] ? map_colors[2] :
               d >= jenks_cutoffs[0] ? map_colors[1] :
                                       map_colors[0];
    }

    function styleGrid(feature){
        return {
            fillColor: getColor(feature.properties.count),
            weight: 0.3,
            opacity: 1,
            color: 'white',
            fillOpacity: 0.7
        };
    }

    function getFieldDefs(dataset_name){
        $.when($.getJSON('/api/fields/' + dataset_name + '/')).then(
            function(fields){
                $('#fielddefs').show();
                $('#fielddefs-list').val()
            }
        )
    }

    function getGrid(url, grid){
        var data = {
            dataset_name: grid['dataset'],
            resolution: grid['resolution'],
            year: grid['year'],
            center: grid['center'],
        }
        return $.getJSON(url, data)
    }
})()
