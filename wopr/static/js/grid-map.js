$(window).resize(function () {
    var h = $(window).height(),
      offsetTop = 105; // Calculate the top offset

    $('#map').css('height', (h - offsetTop));
}).resize();

(function(){
    var grid_layer = new L.FeatureGroup();
    var jenks_cutoffs;
    var map = L.map('map').fitBounds([[41.644286009999995, -87.94010087999999], [42.023134979999995, -87.52366115999999]]);
    L.tileLayer('https://{s}.tiles.mapbox.com/v3/datamade.hn83a654/{z}/{x}/{y}.png', {
        attribution: '<a href="http://www.mapbox.com/about/maps/" target="_blank">Terms &amp; Feedback</a>'
    }).addTo(map);
    var start_date = moment().subtract('days', 372);
    var end_date = moment().subtract('days', 7);
    var min_date = moment().subtract('years', 5);
    var max_date = moment().subtract('days', 7);
    var grid_data = {
        dataset: 'chicago_crimes_all',
        human_name: 'Crimes - 2001 to present',
        resolution: 1000, //in meters
        obs_from: start_date.format('YYYY-MM-DD'),
        obs_to: end_date.format('YYYY-MM-DD')
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
    var drawnItems = new L.FeatureGroup();
    map.addLayer(drawnItems);
    var drawControl = new L.Control.Draw({
        edit: {
            featureGroup: drawnItems
        },
        draw: {
            circle: false,
            marker: false,
            polygon: {
                shapeOptions: {
                    'color': '#123456',
                    'opacity': 0.4,
                    'fillOpacity': 0
                }
            },
            rectangle: {
                shapeOptions: {
                    'color': '#123456',
                    'opacity': 0.4,
                    'fillOpacity': 0
                }
            }
        }
    });
    map.addControl(drawControl);
    map.addControl(new L.Control.Distance()); //distance plugin when drawing lines
    map.addControl(new L.Control.Scale()); //show distance scale in lower left
    map.on('draw:created', draw_create);
    map.on('draw:edited', draw_edit);
    map.on('draw:deleted', draw_delete);
    map.on('draw:drawstart', draw_delete);
    var map_colors = [
        '#deebf7',
        '#c6dbef',
        '#9ecae1',
        '#6baed6',
        '#4292c6',
        '#2171b5',
        '#084594'
    ]
    var date_range_opts = {
        format: 'M/D/YYYY',
        showDropdowns: true,
        startDate: start_date,
        endDate: end_date,
        minDate: min_date,
        maxDate: max_date
    }
    $('#date_range').daterangepicker(
      date_range_opts,
      date_range_callback
    )
    update_date_range();
    $('#dataset-picker').on('change', function(e){
        var name = $(this).val();
        grid_data['dataset'] = name;
        grid_data['human_name'] = $('#dataset-picker').find(':selected').first().text().trim();
        $('.yearpicker').hide();
        $('#' + name + '-yearpicker').show();
        loadLayer(grid_data);
    });
    $('#resolution-picker').on('change', function(e){
        grid_data['resolution'] = parseFloat($(this).val());
        loadLayer(grid_data);
    });
    $('#buffer-picker').on('change', function(e){
        grid_data['buffer'] = parseFloat($(this).val());
        loadLayer(grid_data);
    })
    loadLayer(grid_data);
    $('.showmore').on('click', function(){
        var add_height = $('#dataset-description').height();
        $('.showmore-content').height(add_height);
        $(this).hide()
        $('.showless').show()
    });
    $('.showless').on('click', function(){
        $('.showmore-content').height(165);
        $(this).hide()
        $('.showmore').show()
    })
    function date_range_callback(start, end, label){
        start_date = start;
        end_date = end;
    }
    function draw_create(e){
        if(e.layerType == 'polyline'){
            $('#buffer').show();
        } else {
            $('#buffer').hide();
        }
        drawnItems.addLayer(e.layer);
        grid_data['location_geom__within'] = JSON.stringify(e.layer.toGeoJSON());
        loadLayer(grid_data)
    }
    function draw_edit(e){
        var layers = e.layers;
        layers.eachLayer(function(layer){
            drawnItems.addLayer(layer);
            grid_data['location_geom__within'] = JSON.stringify(layer.toGeoJSON());
        });
    }
    function draw_delete(e){
        grid_layer.clearLayers();
        drawnItems.clearLayers();
    }
    function loadLayer(grid_data){
        $('#map').spin('large');
        grid_data['center'] = [map.getCenter().lat, map.getCenter().lng]
        var url ='/api/grid/'
        $.when(getGrid(url, grid_data), $.getJSON('/api/', {'dataset_name': grid_data['dataset']})).then(
            function(grid, meta){
                grid = grid[0];
                meta = meta[0][0];
                $('#map').spin(false);
                var values = [];
                $.each(grid['features'], function(i, val){
                    values.push(val['properties']['count']);
                });
                try{legend.removeFrom(map);}catch(e){};
                grid_layer.clearLayers();
                if (typeof grid_layer !== 'undefined'){
                    map.removeLayer(grid_layer);
                }
                // Should probably do something here to let users know
                // that there are no results
                if (values.length > 0){
                    jenks_cutoffs = jenks(values, 6);
                    jenks_cutoffs[0] = 0;
                    jenks_cutoffs.pop();
                    grid_layer.addLayer(L.geoJson(grid, {
                        style: styleGrid,
                        onEachFeature: function(feature, layer){
                            var content = '<h4>Count: ' + feature.properties.count + '</h4>';
                            layer.bindLabel(content);
                        }
                    })).addTo(map);
                    legend.addTo(map);
                    map.fitBounds(grid_layer.getBounds());
                }
                if (meta.obs_from){
                    min_date = moment(meta['obs_from'], 'YYYY-MM-DD');
                } else {
                    min_date = moment().subtract('years', 5);
                }
                if (meta.obs_to){
                    max_date = moment(meta['obs_to'], 'YYYY-MM-DD');
                } else {
                    max_date = moment();
                }
                if (end_date.isAfter(max_date)){
                    end_date = max_date;
                }
                if (start_date.isBefore(min_date)){
                    start_state = min_date;
                }
                date_range_opts['minDate'] = min_date
                date_range_opts['maxDate'] = max_date
                date_range_opts['startDate'] = start_date
                date_range_opts['endDate'] = end_date
                $('#date_range').daterangepicker(date_range_opts, date_range_callback);
                $('#date_range').on('apply.daterangepicker', function(ev, picker) {
                    start_date = picker.startDate
                    end_date = picker.endDate
                    grid_data['obs_from'] = start_date.format('YYYY-MM-DD')
                    grid_data['obs_to'] = end_date.format('YYYY-MM-DD')
                    loadLayer(grid_data);
                });
                update_date_range();
                $('#dataset-name').html(meta['human_name']);
                $('#dataset-description').html(meta['description']);
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
            center: grid['center'],
            obs_date__ge: grid['obs_from'],
            obs_date__le: grid['obs_to']
        }
        if (typeof grid['location_geom__within'] !== 'undefined'){
            data['location_geom__within'] = grid['location_geom__within']
        }
        if (typeof grid['buffer'] !== 'undefined'){
            data['buffer'] = grid['buffer']
        }
        return $.getJSON(url, data)
    }
    function update_date_range(){
        $('#date_range').val(start_date.format('M/D/YYYY') + " - " + end_date.format('M/D/YYYY'));
    }
})()
