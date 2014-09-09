var ErrorView = Backbone.View.extend({
    initialize: function(){
        this.render()
    },
    render: function(){
        this.$el.html(template_cache('modalTemplate', this.model));
        this.$el.modal();
        return this;
    }
});