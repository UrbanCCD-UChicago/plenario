var ResponseView = Backbone.View.extend({
    events: {
        'click .detail': 'detailView'
    },
    render: function(){
        $('#list-view').show();
        $('#detail-view').hide();
        var self = this;
        this.query = this.attributes.query;
        if (typeof this.explore !== 'undefined'){
            this.explore.remove();
        }
        this.$el.empty();
        this.charts = {};
        this.$el.spin('large');
        this.getResults();
    },
    detailView: function(e){
        var dataset_name = $(e.target).data('dataset_name')
        this.query['dataset_name'] = dataset_name
        $('#map-view').empty();
        new DetailView({el: '#map-view', attributes: {query: this.query, meta: this.meta[dataset_name]}})
        var route = 'detail/' + $.param(this.query)
        router.navigate(route)
    },
    getResults: function(){
        var self = this;
        $.when(this.resultsFetcher(), this.metaFetcher()).then(
            function(resp, meta_resp){
                self.$el.spin(false);
                var results = resp[0].objects;
                var m = meta_resp[0]
                var objects = []
                self.meta = {}
                $.each(m, function(i, obj){
                    self.meta[obj.dataset_name] = obj
                })
                $.each(results, function(i, obj){
                    obj['values'] = []
                    $.each(obj.items, function(i, o){
                        obj['values'].push([moment(o.datetime + "+0000").valueOf(),o.count]);
                    });
                    // console.log(obj['values'])
                    obj['meta'] = self.meta[obj['dataset_name']]
                    objects.push(obj)
                });

                self.$el.html(template_cache('datasetTable', {
                    objects: objects,
                    query: self.query
                }));
                $.each(objects, function(i, obj){
                    ChartHelper.sparkline((obj['dataset_name'] + '-sparkline'), obj.temporal_aggregate, obj['values']);
                });

                $('#response-datasets').DataTable( {
                    "aaSorting": [ [0,'asc'] ],
                    "aoColumns": [
                        null,
                        { "bSortable": false },
                        { "bSortable": false }
                    ],
                    "paging": false,
                    "searching": false,
                    "info": false
                } );
            }
        ).fail(function(resp){
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
    },
    metaFetcher: function(){
        return $.ajax({
            url: '/api/',
            dataType: 'json'
        })
    }
});