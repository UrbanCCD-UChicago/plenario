jQuery.extend( jQuery.fn.dataTableExt.oSort, {
    "datetime-pre": function ( a ) {
        return a.match(/datetime="*([0-9\-\.]+)/)[1];
    },
 
    "datetime-asc": function ( a, b ) {
        return ((a < b) ? -1 : ((a > b) ? 1 : 0));
    },
 
    "datetime-desc": function ( a, b ) {
        return ((a < b) ? 1 : ((a > b) ? -1 : 0));
    },

    "num-html-pre": function ( a ) {
        return parseFloat( a.replace( /<.*?>/g, "" ).replace("$","").replace(",","") );
    },
    
    "num-html-asc": function ( a, b ) {
        return ((a < b) ? -1 : ((a > b) ?  1 : 0));
    },
    
    "num-html-desc": function ( a, b ) {
        return ((a < b) ?  1 : ((a > b) ? -1 : 0));
    }
} );