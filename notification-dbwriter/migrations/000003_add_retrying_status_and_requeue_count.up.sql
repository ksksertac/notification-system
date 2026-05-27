-- Add 'retrying' status and requeue_count column

-- Drop and recreate CHECK constraint to include 'retrying' status
ALTER TABLE notifications DROP CONSTRAINT IF EXISTS notifications_status_check;
ALTER TABLE notifications ADD CONSTRAINT notifications_status_check
    CHECK (status IN ('pending', 'queued', 'processing', 'delivered', 'retrying', 'failed', 'cancelled'));

-- Add requeue_count column for circuit breaker re-enqueue tracking
ALTER TABLE notifications ADD COLUMN IF NOT EXISTS requeue_count SMALLINT NOT NULL DEFAULT 0;

-- Drop old retry index (filtered on status='failed') and create new one (filtered on status='retrying')
DROP INDEX IF EXISTS idx_notifications_retry;
CREATE INDEX idx_notifications_retry ON notifications(next_retry_at) WHERE status = 'retrying' AND retry_count < max_retries;
