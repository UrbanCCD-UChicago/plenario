(function(){
    var drawnItems = new L.FeatureGroup();
    var map;
    var geojson = new L.LayerGroup();
    $(document).ready(function(){
        resize_junk();
        window.onresize = function(event){
            resize_junk();
        }
        map = L.map('map').setView([41.880517,-87.644061], 13);
        L.tileLayer('http://{s}.tile.cloudmade.com/{key}/{styleId}/256/{z}/{x}/{y}.png', {
          attribution: 'Mapbox <a href="http://mapbox.com/about/maps" target="_blank">Terms &amp; Feedback</a>',
          key: 'BC9A493B41014CAABB98F0471D759707',
          styleId: 22677
        }).addTo(map);
        map.addLayer(drawnItems);
        var drawControl = new L.Control.Draw({
            edit: {
                    featureGroup: drawnItems
            },
            draw: {
                polyline: false,
                circle: false,
                marker: false
            }
        });
        map.addControl(drawControl);
        map.on('draw:created', draw_create);
        map.on('draw:edited', draw_edit);
        map.on('draw:deleted', draw_delete);
        $.when(get_datasets()).then(
            function(resp){
                var tpl = new EJS({url: '/static/js/templates/datasetPicker.ejs'});
                $('#dataset-picker').html(tpl.render({datasets: resp}));
            }
        );
        var filtpl = new EJS({url: '/static/js/templates/filterTemplate.ejs'})
        $('#filters').html(filtpl.render({}));
        $('#dataset').on('change', function(){
            console.log('OK heres where we show and hide info about the datasets')
        })
    });

    function draw_create(e){
        edit_create(e.layer, e.target);
    }

    function draw_edit(e){
        var layers = e.layers;
        geojson.clearLayers();
        layers.eachLayer(function(layer){
            edit_create(layer, e.target);
        });
    }

    function draw_delete(e){
        geojson.clearLayers();
    }

    function edit_create(layer, map){
        $('#map').spin('large');
        var query = {};
        query['geom__within'] = JSON.stringify(layer.toGeoJSON());
        var start = $('.start').val().replace('Start Date: ', '');
        var end = $('.end').val().replace('End Date: ', '');
        start = moment(start)
        end = moment(end)
        var valid = false;
        if (start.isValid() && end.isValid()){
            start = start.startOf('day').unix();
            end = end.endOf('day').unix();
            valid = true;
        }
        //query['date__lte'] = end;
        //query['date__gte'] = start;
       //var on = [];
       //var type_checkboxes = $('.filter.type');
       //$.each(type_checkboxes, function(i, checkbox){
       //    if($(checkbox).is(':checked')){
       //        on.push($(checkbox).attr('value'));
       //    }
       //});
       //query['type'] = on.join(',')
       //on = [];
       //var time_checkboxes = $('.filter.time');
       //$.each(time_checkboxes, function(i, checkbox){
       //    if($(checkbox).is(':checked')){
       //        on.push($(checkbox).attr('value'));
       //    }
       //});
       //query['time'] = on.join(',')
        var marker_opts = {
            radius: 10,
            weight: 2,
            opacity: 1,
            fillOpacity: 0.6
        };
        if(valid){
            $.when(get_results(query)).then(function(resp){
                $('#map').spin(false);
                $.each(resp.objects, function(i, result){
                    var location = result.geom;
                    location.properties = result;
                    geojson.addLayer(L.geoJson(location, {
                        pointToLayer: function(feature, latlng){
                            marker_opts.color = '#7B3294';
                            marker_opts.fillColor = '#7B3294';
                            return L.circleMarker(latlng, marker_opts)
                        }//,
                        //onEachFeature: bind_popup
                    })).addTo(map);
                });
            }).fail(function(data){
                $('#map').spin(false);
                var error = {
                    header: 'Woops!',
                    body: data['responseJSON']['meta']['message'],
                }
                var errortpl = new EJS({url: '/static/js/templates/modalTemplate.ejs'})
                $('#errorModal').html(errortpl.render(error));
                $('#errorModal').modal();
            })
        } else {
            $('#map').spin(false);
            $('#date-error').reveal();
        }
        drawnItems.addLayer(layer);
    }
    function bind_popup(feature, layer){
        var crime_template = new EJS({url: 'js/views/dataTemplate.ejs'});
        var props = feature.properties;
        var pop_content = crime_template.render(props);
        layer.bindPopup(pop_content, {
            closeButton: true,
            minWidth: 320
        })
    }
    function resize_junk(){
        $('.full-height').height(window.innerHeight - 40);
    }

    function get_datasets(){
        return $.ajax({
            url: '/api/',
            dataType: 'json'
        })
    }

    function get_results(query){
        var dataset = $('#dataset').val();
        return $.ajax({
            url: '/api/' + dataset + '/',
            dataType: 'json',
            data: query
        });
    }
})()
