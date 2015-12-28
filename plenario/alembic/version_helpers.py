def dataset_names(op):
    conn = op.get_bind()
    sel = "SELECT dataset_name FROM meta_master WHERE approved_status = 'true';"

    return [row['dataset_name']for row in conn.execute(sel)]
