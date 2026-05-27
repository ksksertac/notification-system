-- Revert 'retrying' status and requeue_count column

-- Revert retry index to filter on 'failed'
DROP INDEX IF EXISTS idx_notifications_retry;
CREATE INDEX idx_notifications_retry ON notifications(next_retry_at) WHERE status = 'failed' AND retry_count < max_retries;

-- Remove requeue_count column
ALTER TABLE notifications DROP COLUMN IF EXISTS requeue_count;

-- Revert CHECK constraint to exclude 'retrying'
ALTER TABLE notifications DROP CONSTRAINT IF EXISTS notifications_status_check;
ALTER TABLE notifications ADD CONSTRAINT notifications_status_check
    CHECK (status IN ('pending', 'queued', 'processing', 'delivered', 'failed', 'cancelled'));
