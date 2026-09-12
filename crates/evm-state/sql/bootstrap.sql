-- Private replay accumulator. None of these rows is a published checkpoint.
CREATE TABLE IF NOT EXISTS bootstrap_storage (
    generation String, address String, slot String, value String
) ENGINE=MergeTree PARTITION BY generation ORDER BY (generation,address,slot)
SETTINGS fsync_after_insert=1, fsync_part_directory=1;

CREATE TABLE IF NOT EXISTS bootstrap_generations (
    generation String, manifest String
) ENGINE=MergeTree PARTITION BY generation ORDER BY generation
SETTINGS fsync_after_insert=1, fsync_part_directory=1;
