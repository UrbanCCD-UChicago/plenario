## Scripts to migrate database away from Master Table.

 1. Remove dat_ prefix everywhere
 2. Impose unique dataset name constraint on MetaMaster table.
 3. Add geometry and date columns to individual datasets.
 4. Populate date and geom columns with records from master.
 5. Deduplicate dup_ver and makke it the primary key.
 6. Remove [dataset_name]_row_id, dup_ver, start_date, end_date, current_flag.
 7. Add indexes on geometry and date.