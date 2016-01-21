function validateEmail(email) { 
    var re = /^(([^<>()[\]\\.,;:\s@\"]+(\.[^<>()[\]\\.,;:\s@\"]+)*)|(\".+\"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;
    return re.test(email);
} 

$('#datatype-submit').on('click', function(e){
    function appendError(message) {
        $('#error-list').append(message + '<br />');
    }

    var definitions = {
        'field_definitions': {},
        'data_types': []
    }
    var valid = true;
    $('#errors').hide();
    $('#error-list').empty();
    $.each($('.plenario-field option'), function(i, opt){
        var field_name = $(opt).val();
        if($(opt).is(':selected')){
            var field_type = $(opt).text().split(' ').join('_').toLowerCase().replace(/-/g, '');
            if (field_type){
                if (typeof definitions['field_definitions'][field_type] !== 'undefined'){
                    valid = false
                    appendError('You defined more than one ' + $(opt).text());
                } else {
                    definitions['field_definitions'][field_type] = field_name;
                }
            }
        }
    });

    $.each($('.data-type option'), function(i, opt){
    var field_name = $(opt).val();
        if($(opt).is(':selected')){
            var d_type = $(opt).text().split(' ').join('_').toLowerCase().replace(/-/g, "")
            if(d_type){
                definitions['data_types'].push({
                    'field_name': field_name,
                    'data_type': d_type
                })
            } else {
                valid = false;
                appendError('Provide a data type for ' + field_name);
            }
        }

    });

    var defined = []
    $.each(definitions['field_definitions'], function(i, definition){
        defined.push(i);
    })
    if(!(defined.indexOf('unique_id') >= 0)){
        valid = false
        appendError('You need to define a unique ID field');
    }
    if(!(defined.indexOf('observation_date') >= 0)){
        valid = false
        appendError('You need to define a date/datetime field');
    }
    if(!(defined.indexOf('location') >= 0)){
        if(!(defined.indexOf('latitude') >= 0) && !(defined.indexOf('longitude') >= 0)){
            valid = false
            appendError('You need to defined either a location field or a latitude and longitude field');
        }
    }
    if(!$('#update_frequency').val()){
        valid = false
        appendError('You need to specify how often should we check for updates');
    } else {
        definitions['update_frequency'] = $('#update_frequency').val();
    }
        
    if($('#contributor_name').val() == "") {
        valid = false;
        appendError('You need to provide your name');
    }

    if (!validateEmail($('#contributor_email').val())) {
         valid= false;
         appendError('"' + $('#contributor_email').val() + '" is not a valid email address');
    }

    if(!valid) {
        window.scrollTo(0,0);
        $('#errors').show();
        e.preventDefault();
    }
})