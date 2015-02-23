var ResponseView = Backbone.View.extend({
    events: {
        'click .detail': 'detailView'
    },
    initialize: function(){
        this.render();
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
        // console.log('response-view detailView')
        var dataset_name = $(e.target).data('dataset_name')
        this.query['dataset_name'] = dataset_name
        this.undelegateEvents();
        $('#map-view').empty();
        new DetailView({el: '#map-view', attributes: {query: this.query, meta: this.meta[dataset_name]}})
        var route = 'detail/' + $.param(this.query)
        _gaq.push(['_trackPageview', route]);
        router.navigate(route)
    },
    getResults: function(){
        var self = this;
        $.when(this.resultsFetcher(), this.metaFetcher()).then(
            function(resp, meta_resp){
                self.$el.spin(false);
                var results = resp[0].objects;
                var results_meta = resp[0]['meta']
                var m = meta_resp[0]['objects']
                var objects = []
                self.meta = {}
                $.each(m, function(i, obj){
                    self.meta[obj.dataset_name] = obj
                })
                $.each(results, function(i, obj){
                    obj['values'] = []
                    obj['count'] = 0;
                    $.each(obj.items, function(i, o){
                        obj['values'].push([moment(o.datetime + "+0000").valueOf(),o.count]);
                        obj['count'] += o.count;
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
                    ChartHelper.sparkline((obj['dataset_name'] + '-sparkline'), results_meta['query']['agg'], obj['values']);
                });

                $('#response-datasets').DataTable( {
                    "aaSorting": [ [2,'desc'] ],
                    "aoColumns": [
                        null,
                        null,
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
            var error = {
                header: 'Woops!',
                body: "Error fetching data.",
            }
            new ErrorView({el: '#errorModal', model: error});
        });
    },
    resultsFetcher: function(){
        var self = this;
        return $.ajax({
            url: '/v1/api/timeseries/',
            dataType: 'json',
            data: self.query
        });
    },
    metaFetcher: function(){
        return $.ajax({
            url: '/v1/api/datasets/',
            dataType: 'json'
        })
    }
});
