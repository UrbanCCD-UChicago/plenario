var map;
var geojson = null;
var results = null;
var resp;

var router = new AppRouter();
Backbone.history.start();
Backbone.Model.extend({'foo': 'bar'});
console.log('Can extend Model');
Backbone.Collection.extend({'fizz': 'gorp'});
console.log('Can extend Collection');
