var ErrorView = Backbone.View.extend({
    initialize: function(){
        this.render()
    },
    render: function(){
        this.$el.html(template_cache('errorTemplate', this.model));
        this.$el.modal();
        return this;
    }
});