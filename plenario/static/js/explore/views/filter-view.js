var FilterView = Backbone.View.extend({
    events: {
        'click .remove-filter': 'clear'
    },
    initialize: function(){
        // console.log(this.attributes);
        this.filter_dict = this.attributes.filter_dict;
        this.field_options = this.attributes.field_options;
        this.render();
    },
    render: function(){
        this.$el.append(_.template(get_template('filterTemplate'))(this.filter_dict));

        var filter_dict_id = this.filter_dict.id;
        $.each(this.field_options['objects'], function(k, v){
            $('#field_' + filter_dict_id).append("<option value='" + v['field_name'] + "'>" + humanize(v['field_name']) + "</option>");
        });

        // select dropdowns
        $("#field_" + this.filter_dict.id).val(this.filter_dict.field);
        $("#operator_" + this.filter_dict.id).val(this.filter_dict.operator);
    },
    clear: function(e){
        $("#row_" + $(e.currentTarget).attr("data-id")).remove();
    }
});