CREATE TABLE IF NOT EXISTS deal_state (
  deal_id           BIGINT PRIMARY KEY,
  last_stage_id     VARCHAR(100) NULL,
  last_sent_hash    CHAR(64) NULL,
  locked_counter_id BIGINT NULL,
  locked_mp_token   VARCHAR(255) NULL,
  locked_uf_value   VARCHAR(255) NULL,
  updated_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS metrika_queue (
  id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  deal_id     BIGINT NOT NULL,
  event_type  VARCHAR(32) NOT NULL,
  payload     JSON NOT NULL,
  status      VARCHAR(16) NOT NULL DEFAULT 'queued',
  attempts    INT NOT NULL DEFAULT 0,
  last_error  TEXT NULL,
  created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  sent_at     TIMESTAMP NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX IF NOT EXISTS metrika_queue_status_idx ON metrika_queue(status);

CREATE TABLE IF NOT EXISTS metrika_routing (
  uf_value   VARCHAR(255) PRIMARY KEY,
  counter_id BIGINT NOT NULL,
  mp_token   VARCHAR(255) NOT NULL,
  is_active  TINYINT(1) NOT NULL DEFAULT 1,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
