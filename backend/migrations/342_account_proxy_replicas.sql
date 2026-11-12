-- Durable operation identity is independent of the expiring generic HTTP
-- idempotency cache. No OAuth token is stored in these tables.
CREATE TABLE IF NOT EXISTS account_proxy_replica_operations (
    operation_id varchar(200) PRIMARY KEY,
    source_account_id bigint NOT NULL,
    config_hash varchar(64) NOT NULL,
    config jsonb NOT NULL,
    completed boolean NOT NULL DEFAULT false,
    status varchar(20) NOT NULL DEFAULT 'pending',
    error text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS account_proxy_replica_items (
    operation_id varchar(200) NOT NULL REFERENCES account_proxy_replica_operations(operation_id),
    proxy_id bigint NOT NULL,
    account_id bigint NOT NULL UNIQUE,
    PRIMARY KEY(operation_id, proxy_id)
);
CREATE INDEX IF NOT EXISTS accounts_proxy_replica_owner_idx
    ON accounts ((extra->>'credential_owner_account_id'))
    WHERE extra ? 'credential_owner_account_id';
CREATE UNIQUE INDEX IF NOT EXISTS accounts_proxy_replica_owner_proxy_idx
    ON accounts ((extra->>'credential_owner_account_id'), proxy_id)
    WHERE deleted_at IS NULL AND extra ? 'credential_owner_account_id';

-- A source change publishes child invalidation in the same transaction,
-- including direct SQL / soft deletion used by external account maintenance.
-- Runtime admission additionally reads the primary DB before every send.
CREATE OR REPLACE FUNCTION propagate_proxy_replica_owner_change() RETURNS trigger AS $$
DECLARE owner_id bigint;
BEGIN
    owner_id := OLD.id;
    -- Unrelated account writes must not scan or publish replica state.
    IF OLD.extra->>'proxy_replica_source' IS DISTINCT FROM 'true' THEN
        IF TG_OP='DELETE' THEN RETURN OLD; END IF;
        IF NEW.extra->>'proxy_replica_source' IS DISTINCT FROM 'true' THEN RETURN NEW; END IF;
    END IF;
    IF OLD.extra ? 'credential_owner_account_id' THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        UPDATE accounts SET schedulable=false, status='disabled', updated_at=NOW()
        WHERE extra ? 'credential_owner_account_id' AND extra->>'credential_owner_account_id'=owner_id::text AND deleted_at IS NULL;
    ELSE
        IF NEW.deleted_at IS NOT NULL OR NEW.status <> 'active' OR
           (NEW.expires_at IS NOT NULL AND NEW.expires_at <= NOW()) THEN
            UPDATE accounts SET schedulable=false, updated_at=NOW()
            WHERE extra ? 'credential_owner_account_id' AND extra->>'credential_owner_account_id'=owner_id::text AND deleted_at IS NULL;
        END IF;
    END IF;
    INSERT INTO scheduler_outbox(event_type, account_id)
    SELECT 'account_changed', id FROM accounts
    WHERE extra ? 'credential_owner_account_id' AND extra->>'credential_owner_account_id'=owner_id::text;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS proxy_replica_owner_change ON accounts;
CREATE TRIGGER proxy_replica_owner_change AFTER UPDATE OR DELETE ON accounts
FOR EACH ROW EXECUTE FUNCTION propagate_proxy_replica_owner_change();

-- Never let periodic recovery, bulk scheduling or import turn an owner into
-- an additional direct entry. Keep association immutable, even for raw writers.
CREATE OR REPLACE FUNCTION guard_proxy_replica_account() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF OLD.extra ? 'credential_owner_account_id' THEN
            IF NEW.extra->'credential_owner_account_id' IS DISTINCT FROM OLD.extra->'credential_owner_account_id' OR
               NEW.extra->'proxy_replica_operation_id' IS DISTINCT FROM OLD.extra->'proxy_replica_operation_id' OR
               NEW.platform <> OLD.platform OR NEW.type <> OLD.type OR
               NEW.proxy_id IS DISTINCT FROM OLD.proxy_id THEN
                RAISE EXCEPTION 'proxy replica binding is immutable';
            END IF;
        END IF;
        IF OLD.extra->>'proxy_replica_source' = 'true' THEN
            NEW.extra := NEW.extra || '{"proxy_replica_source":true}'::jsonb;
        END IF;
    END IF;
    IF NEW.extra->>'proxy_replica_source' = 'true' THEN
        NEW.schedulable := false;
    END IF;
    IF NEW.extra ? 'credential_owner_account_id' THEN
        IF TG_OP='INSERT' OR NOT (OLD.extra ? 'credential_owner_account_id') THEN
            IF current_setting('sub2api.proxy_replica_operation',true) IS DISTINCT FROM NEW.extra->>'proxy_replica_operation_id' THEN
                RAISE EXCEPTION 'proxy replicas must be created by the replica operation';
            END IF;
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM account_proxy_replica_operations o
            WHERE o.operation_id=NEW.extra->>'proxy_replica_operation_id'
              AND o.source_account_id::text=NEW.extra->>'credential_owner_account_id'
        ) THEN
            RAISE EXCEPTION 'proxy replica requires a durable operation';
        END IF;
        IF NEW.proxy_id IS NULL OR NEW.parent_account_id IS NOT NULL OR NEW.platform <> 'openai' OR NEW.type <> 'oauth' THEN
            RAISE EXCEPTION 'invalid proxy replica binding';
        END IF;
        -- Only routing/model configuration is stored on the replica.
        NEW.credentials := (SELECT COALESCE(jsonb_object_agg(key,value),'{}'::jsonb)
            FROM jsonb_each(NEW.credentials)
            WHERE key IN ('model_mapping','compact_model_mapping','model_mapping_mode','plan_type',
                          'chatgpt_account_id','chatgpt_user_id','organization_id','email'));
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS proxy_replica_account_guard ON accounts;
CREATE TRIGGER proxy_replica_account_guard BEFORE INSERT OR UPDATE ON accounts
FOR EACH ROW EXECUTE FUNCTION guard_proxy_replica_account();

CREATE OR REPLACE FUNCTION invalidate_proxy_replica_proxy() RETURNS trigger AS $$
BEGIN
    IF NEW.status <> 'active' OR NEW.deleted_at IS NOT NULL OR
       (NEW.expires_at IS NOT NULL AND NEW.expires_at <= NOW()) THEN
        WITH paused AS (
            UPDATE accounts SET schedulable=false,updated_at=NOW()
            WHERE proxy_id=NEW.id AND extra ? 'credential_owner_account_id'
              AND deleted_at IS NULL RETURNING id
        )
        INSERT INTO scheduler_outbox(event_type,account_id)
        SELECT 'account_changed',id FROM paused;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS proxy_replica_proxy_change ON proxies;
CREATE TRIGGER proxy_replica_proxy_change AFTER UPDATE ON proxies
FOR EACH ROW EXECUTE FUNCTION invalidate_proxy_replica_proxy();
