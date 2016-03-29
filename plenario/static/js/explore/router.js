var AppRouter = Backbone.Router.extend({
    routes: {
        "": "defaultRoute",
        "city/:city": "defaultRoute",
        "aggregate/:query": "aggregate",
        "detail/:query": "detail",
        "shapeDetail/:query":"shapeDetail"
    },
    defaultRoute: function(city){
        if (city === undefined) {
            city = 'chicago';
        }
        city = city.toLowerCase();

        new AboutView({el: '#list-view'});
        shapeView = new app.ShapeView({el: '#shapes-view'});
        map = new MapView({el: '#map-view', attributes: {
            city: city
        }})
    },
    aggregate: function(query){
        var q = parseParams(query);
        resp = new ResponseView({el: '#list-view', attributes: {query: q}});
        var attrs = {
            resp: resp
        };
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
    shapeDetail: function(query){
        var q = parseParams(query);
        var collection = new app.Shapes();
        collection.fetch({reset:true,success:function(){
            shapeDetailView = new app.ShapeDetailView({el: '#map-view', model:collection.get(q['shape_dataset_name']),query:q});}
        });
    }

});