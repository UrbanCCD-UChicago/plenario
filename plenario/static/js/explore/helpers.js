function get_template(tmpl_name){
    var tmpl_dir = '/static/js/explore/templates';
    var tmpl_url = tmpl_dir + '/' + tmpl_name + '.html';

    var tmpl_string = "";
    $.ajax({
        url: tmpl_url,
        method: 'GET',
        async: false,
        success: function(data) {
            tmpl_string = data;
        }
    });

    return tmpl_string;
}

function template_cache(tmpl_name, tmpl_data){
    if ( !template_cache.tmpl_cache ) {
        template_cache.tmpl_cache = {};
    }

    if ( ! template_cache.tmpl_cache[tmpl_name] ) {
        var tmpl_string = get_template(tmpl_name);
        template_cache.tmpl_cache[tmpl_name] = _.template(tmpl_string);
    }

    return template_cache.tmpl_cache[tmpl_name](tmpl_data);
}

function parseParams(query){
    var re = /([^&=]+)=?([^&]*)/g;
    var decodeRE = /\+/g;  // Regex for replacing addition symbol with a space
    var decode = function (str) {return str.replace(decodeRE, " ");};
    var params = {}, e;
    while ( e = re.exec(query) ) {
        var k = decode( e[1] ), v = decode( e[2] );
        if (k.substring(k.length - 2) === '[]') {
            k = k.substring(0, k.length - 2);
            (params[k] || (params[k] = [])).push(v);
        }
        else params[k] = v;
    }
    return params;
}

function humanize(property) {
  return property.replace(/_/g, ' ')
      .replace(/(\w+)/g, function(match) {
        return match.charAt(0).toUpperCase() + match.slice(1);
      });
};

function addCommas(nStr) {
    nStr += '';
    x = nStr.split('.');
    x1 = x[0];
    x2 = x.length > 1 ? '.' + x[1] : '';
    var rgx = /(\d+)(\d{3})/;
    while (rgx.test(x1)) {
      x1 = x1.replace(rgx, '$1' + ',' + '$2');
    }
    return x1 + x2;
}