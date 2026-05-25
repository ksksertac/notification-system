CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE notifications (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    idempotency_key VARCHAR(255) UNIQUE,
    batch_id        UUID,
    recipient       VARCHAR(255) NOT NULL,
    channel         VARCHAR(10)  NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
    content         TEXT         NOT NULL,
    priority        SMALLINT     NOT NULL DEFAULT 1 CHECK (priority IN (0, 1, 2)),
    status          VARCHAR(20)  NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'queued', 'processing', 'delivered', 'failed', 'cancelled')),
    provider_msg_id VARCHAR(255),
    retry_count     SMALLINT     NOT NULL DEFAULT 0,
    max_retries     SMALLINT     NOT NULL DEFAULT 5,
    next_retry_at   TIMESTAMPTZ,
    scheduled_at    TIMESTAMPTZ,
    metadata        JSONB        DEFAULT '{}',
    error_message   TEXT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_notifications_status ON notifications(status);
CREATE INDEX idx_notifications_batch_id ON notifications(batch_id) WHERE batch_id IS NOT NULL;
CREATE INDEX idx_notifications_channel_status ON notifications(channel, status);
CREATE INDEX idx_notifications_scheduled ON notifications(scheduled_at) WHERE status = 'pending' AND scheduled_at IS NOT NULL;
CREATE INDEX idx_notifications_retry ON notifications(next_retry_at) WHERE status = 'failed' AND retry_count < max_retries;
CREATE INDEX idx_notifications_created_at ON notifications(created_at);
CREATE INDEX idx_notifications_idempotency ON notifications(idempotency_key) WHERE idempotency_key IS NOT NULL;

CREATE TABLE dead_letter_queue (
    id                UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    notification_id   UUID NOT NULL REFERENCES notifications(id),
    channel           VARCHAR(10)  NOT NULL,
    recipient         VARCHAR(255) NOT NULL,
    content           TEXT         NOT NULL,
    error_message     TEXT,
    retry_count       SMALLINT     NOT NULL,
    failed_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    reprocessed       BOOLEAN      NOT NULL DEFAULT FALSE,
    reprocessed_at    TIMESTAMPTZ
);

CREATE INDEX idx_dlq_reprocessed ON dead_letter_queue(reprocessed) WHERE reprocessed = FALSE;
CREATE INDEX idx_dlq_notification_id ON dead_letter_queue(notification_id);
