CREATE TABLE `amazon-ppc-474902.amazon_ppc.bid_change_log` (
  change_id STRING NOT NULL,
  keyword_id STRING,
  old_bid FLOAT64,
  new_bid FLOAT64,
  bid_change FLOAT64,
  reason STRING,
  changed_by STRING,
  components STRING,
  changed_at TIMESTAMP,
  dry_run BOOL
);
