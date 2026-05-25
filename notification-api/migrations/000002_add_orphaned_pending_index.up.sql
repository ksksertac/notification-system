CREATE INDEX idx_notifications_orphaned_pending
    ON notifications(updated_at)
    WHERE status = 'pending' AND scheduled_at IS NULL;
