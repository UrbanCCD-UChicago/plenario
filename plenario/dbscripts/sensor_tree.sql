create or replace function network_tree(network varchar)
  returns json as $$

  var query = 'select * from sensor__sensor_to_node as stn ';
  query += 'join sensor__sensor_metadata as sm ';
  query += 'on stn.sensor = sm.name ';
  query += "where stn.network = '" + network + "'";
  var records = plv8.execute(query);

  var tree = {};
  for (var i = 0; i < records.length; i++) {
    var record = records[i];

    var node = record.node;
    if (!(node in tree)) {
      tree[node] = {};
    }
    tree[node][record.sensor] = record.observed_properties;
  }

  return tree;
$$ language plv8;
