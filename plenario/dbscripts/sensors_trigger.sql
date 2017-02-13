create or replace function invert(j jsonb) returns jsonb as $$

    var result = {};
    for (var key in j) {
        result[j[key]] = key;
    }
    return result;
$$ language plv8;

create view sensor__sensors_view as
  select name, invert(observed_properties)
  from sensor__sensor_metadata;
