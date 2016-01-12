var AppRouter = Backbone.Router.extend({
    routes: {
        "": "defaultRoute",
        "aggregate/:query": "aggregate",
        "detail/:query": "detail",
        "shapeDetail/:shape_query":"shapeDetail"
    },
    defaultRoute: function(){
        new AboutView({el: '#list-view'});
        shapeView = new app.ShapeView({el: '#shapes-view'});
        map = new MapView({el: '#map-view', attributes: {}})
    },
    aggregate: function(query){
        var q = parseParams(query);
        resp = new ResponseView({el: '#list-view', attributes: {query: q}});
        var attrs = {
            resp: resp
        }
        if (typeof q['location_geom__within'] !== 'undefined'){
            attrs['dataLayer'] = $.parseJSON(q['location_geom__within']);
        }
        map = new MapView({el: '#map-view', attributes: attrs});
        shapeView = new app.ShapeView({el: '#shapes-view'});
    },
    detail: function(query){
        var q = parseParams(query);
        var dataset = q['dataset_name'];
        $.when($.getJSON('/v1/api/datasets/', {dataset_name: dataset})).then(
            function(resp){
                new DetailView({el: '#map-view', attributes: {query: q, meta: resp['objects'][0]}})
            }
        )
    },
    shapeDetail: function(shape_query){
        var q = parseParams(shape_query);
        shapeDetailView = new app.ShapeDetailView({el: '#map-view', shape_query: q});
    }

});