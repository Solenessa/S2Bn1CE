PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS scan_runs (
  id INTEGER PRIMARY KEY,
  root_path TEXT NOT NULL,
  started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  finished_at TEXT
);

CREATE TABLE IF NOT EXISTS files (
  id INTEGER PRIMARY KEY,
  scan_run_id INTEGER NOT NULL,
  root_path TEXT NOT NULL,
  relative_path TEXT NOT NULL,
  file_name TEXT NOT NULL,
  extension TEXT NOT NULL,
  size_bytes INTEGER NOT NULL,
  modified_at REAL NOT NULL,
  sha256 TEXT NOT NULL,
  is_package INTEGER NOT NULL DEFAULT 0,
  parse_status TEXT NOT NULL DEFAULT 'not_attempted',
  parse_error TEXT,
  dbpf_major INTEGER,
  dbpf_minor INTEGER,
  index_major INTEGER,
  index_minor INTEGER,
  resource_count INTEGER NOT NULL DEFAULT 0,
  duplicate_group_key TEXT,
  FOREIGN KEY (scan_run_id) REFERENCES scan_runs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_files_scan_run_id ON files(scan_run_id);
CREATE INDEX IF NOT EXISTS idx_files_sha256 ON files(sha256);
CREATE INDEX IF NOT EXISTS idx_files_relative_path ON files(relative_path);
CREATE INDEX IF NOT EXISTS idx_files_extension ON files(extension);
CREATE INDEX IF NOT EXISTS idx_files_parse_status ON files(parse_status);

CREATE TABLE IF NOT EXISTS package_resources (
  id INTEGER PRIMARY KEY,
  file_id INTEGER NOT NULL,
  type_id INTEGER NOT NULL,
  group_id INTEGER NOT NULL,
  instance_id INTEGER NOT NULL,
  instance_hi INTEGER,
  file_offset INTEGER NOT NULL,
  file_size INTEGER NOT NULL,
  body_sha256 TEXT,
  resource_key TEXT NOT NULL,
  type_label TEXT,
  is_dir_record INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_package_resources_file_id ON package_resources(file_id);
CREATE INDEX IF NOT EXISTS idx_package_resources_resource_key ON package_resources(resource_key);
CREATE INDEX IF NOT EXISTS idx_package_resources_type_label ON package_resources(type_label);
CREATE INDEX IF NOT EXISTS idx_package_resources_body_sha256 ON package_resources(body_sha256);

CREATE TABLE IF NOT EXISTS objd_objects (
  id INTEGER PRIMARY KEY,
  package_resource_id INTEGER NOT NULL UNIQUE,
  file_id INTEGER NOT NULL,
  resource_key TEXT NOT NULL,
  object_name TEXT,
  version INTEGER,
  guid INTEGER,
  original_guid INTEGER,
  diagonal_guid INTEGER,
  grid_aligned_guid INTEGER,
  proxy_guid INTEGER,
  job_object_guid INTEGER,
  object_model_guid INTEGER,
  interaction_table_id INTEGER,
  object_type INTEGER,
  price INTEGER,
  slot_id INTEGER,
  catalog_strings_id INTEGER,
  function_sort_flags INTEGER,
  room_sort_flags INTEGER,
  expansion_flag INTEGER,
  multi_tile_master_id INTEGER,
  multi_tile_sub_index INTEGER,
  raw_length INTEGER NOT NULL,
  FOREIGN KEY (package_resource_id) REFERENCES package_resources(id) ON DELETE CASCADE,
  FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_objd_objects_guid ON objd_objects(guid);
CREATE INDEX IF NOT EXISTS idx_objd_objects_file_id ON objd_objects(file_id);
CREATE INDEX IF NOT EXISTS idx_objd_objects_object_name ON objd_objects(object_name);

CREATE TABLE IF NOT EXISTS bhav_functions (
  id INTEGER PRIMARY KEY,
  package_resource_id INTEGER NOT NULL UNIQUE,
  file_id INTEGER NOT NULL,
  resource_key TEXT NOT NULL,
  function_name TEXT,
  signature INTEGER,
  instruction_count INTEGER,
  tree_type INTEGER,
  arg_count INTEGER,
  local_var_count INTEGER,
  header_flag INTEGER,
  tree_version INTEGER,
  instruction_length INTEGER,
  first_opcode INTEGER,
  last_opcode INTEGER,
  raw_length INTEGER NOT NULL,
  FOREIGN KEY (package_resource_id) REFERENCES package_resources(id) ON DELETE CASCADE,
  FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_bhav_functions_file_id ON bhav_functions(file_id);
CREATE INDEX IF NOT EXISTS idx_bhav_functions_signature ON bhav_functions(signature);
CREATE INDEX IF NOT EXISTS idx_bhav_functions_instruction_count ON bhav_functions(instruction_count);

CREATE TABLE IF NOT EXISTS ttab_tables (
  id INTEGER PRIMARY KEY,
  package_resource_id INTEGER NOT NULL UNIQUE,
  file_id INTEGER NOT NULL,
  resource_key TEXT NOT NULL,
  instance_id INTEGER NOT NULL,
  format_code INTEGER,
  raw_length INTEGER NOT NULL,
  FOREIGN KEY (package_resource_id) REFERENCES package_resources(id) ON DELETE CASCADE,
  FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_ttab_tables_file_id ON ttab_tables(file_id);
CREATE INDEX IF NOT EXISTS idx_ttab_tables_instance_id ON ttab_tables(instance_id);

CREATE TABLE IF NOT EXISTS gzps_entries (
  id INTEGER PRIMARY KEY,
  package_resource_id INTEGER NOT NULL UNIQUE,
  file_id INTEGER NOT NULL,
  resource_key TEXT NOT NULL,
  name TEXT,
  creator TEXT,
  family TEXT,
  age INTEGER,
  gender INTEGER,
  species INTEGER,
  parts INTEGER,
  outfit INTEGER,
  flags INTEGER,
  product INTEGER,
  genetic INTEGER,
  type_value TEXT,
  skintone TEXT,
  hairtone TEXT,
  category_bin INTEGER,
  raw_length INTEGER NOT NULL,
  FOREIGN KEY (package_resource_id) REFERENCES package_resources(id) ON DELETE CASCADE,
  FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_gzps_entries_file_id ON gzps_entries(file_id);
CREATE INDEX IF NOT EXISTS idx_gzps_entries_name ON gzps_entries(name);
CREATE INDEX IF NOT EXISTS idx_gzps_entries_creator ON gzps_entries(creator);
CREATE INDEX IF NOT EXISTS idx_gzps_entries_family ON gzps_entries(family);

CREATE TABLE IF NOT EXISTS duplicate_groups (
  id INTEGER PRIMARY KEY,
  sha256 TEXT NOT NULL UNIQUE,
  file_count INTEGER NOT NULL,
  total_size_bytes INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS pair_reviews (
  left_file_id INTEGER NOT NULL,
  right_file_id INTEGER NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('confirmed', 'dismissed')),
  note TEXT,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (left_file_id, right_file_id),
  FOREIGN KEY (left_file_id) REFERENCES files(id) ON DELETE CASCADE,
  FOREIGN KEY (right_file_id) REFERENCES files(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS crash_reports (
  id INTEGER PRIMARY KEY,
  source_path TEXT NOT NULL UNIQUE,
  file_name TEXT NOT NULL,
  log_type TEXT NOT NULL CHECK(log_type IN ('crash', 'config')),
  sha256 TEXT NOT NULL,
  occurred_at_text TEXT,
  app_name TEXT,
  exception_code TEXT,
  exception_module TEXT,
  fault_address TEXT,
  crash_category TEXT NOT NULL,
  summary TEXT NOT NULL,
  graphics_device TEXT,
  graphics_vendor TEXT,
  driver_version TEXT,
  texture_memory_mb INTEGER,
  os_version TEXT,
  memory_hint TEXT,
  raw_text TEXT NOT NULL,
  imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_crash_reports_log_type ON crash_reports(log_type);
CREATE INDEX IF NOT EXISTS idx_crash_reports_crash_category ON crash_reports(crash_category);
CREATE INDEX IF NOT EXISTS idx_crash_reports_exception_module ON crash_reports(exception_module);

CREATE TABLE IF NOT EXISTS scenegraph_names (
  id INTEGER PRIMARY KEY,
  file_id INTEGER NOT NULL,
  source_type_label TEXT NOT NULL,
  resource_key TEXT NOT NULL,
  value TEXT NOT NULL,
  normalized_value TEXT NOT NULL DEFAULT '',
  FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_scenegraph_names_file_id ON scenegraph_names(file_id);
CREATE INDEX IF NOT EXISTS idx_scenegraph_names_normalized_value ON scenegraph_names(normalized_value);
CREATE INDEX IF NOT EXISTS idx_scenegraph_names_resource_key ON scenegraph_names(resource_key);

CREATE TABLE IF NOT EXISTS resource_links (
  id INTEGER PRIMARY KEY,
  file_id INTEGER NOT NULL,
  source_type_label TEXT NOT NULL,
  source_resource_key TEXT NOT NULL,
  target_resource_key TEXT NOT NULL,
  target_type_id INTEGER NOT NULL,
  FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_resource_links_file_id ON resource_links(file_id);
CREATE INDEX IF NOT EXISTS idx_resource_links_target_resource_key ON resource_links(target_resource_key);
