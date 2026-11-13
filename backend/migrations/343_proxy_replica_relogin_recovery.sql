-- Recovery metadata is independent of credentials and of the HTTP cache.
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS proxy_recovery_revision bigint NOT NULL DEFAULT 0;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS proxy_credential_generation text;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS proxy_verified_generation text;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS proxy_verified_until timestamptz;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS proxy_auth_fault text;
UPDATE accounts SET proxy_credential_generation=gen_random_uuid()::text
WHERE extra->>'proxy_replica_source'='true' AND proxy_credential_generation IS NULL;

CREATE TABLE account_proxy_recovery_pauses (
    account_id bigint PRIMARY KEY, source_account_id bigint NOT NULL,
    fault text NOT NULL, credential_generation text NOT NULL,
    revision bigint NOT NULL, revoked_reason text NOT NULL DEFAULT ''
);
CREATE INDEX account_proxy_recovery_pauses_source ON account_proxy_recovery_pauses(source_account_id);
CREATE TABLE account_proxy_recoveries (
    operation_id varchar(200) PRIMARY KEY, source_account_id bigint NOT NULL,
    config_hash text NOT NULL, config jsonb NOT NULL,
    baseline_generation text NOT NULL, pauses jsonb NOT NULL,
    result jsonb, created_at timestamptz NOT NULL DEFAULT NOW(),
    expires_at timestamptz NOT NULL DEFAULT NOW()+INTERVAL '15 minutes'
);

CREATE FUNCTION proxy_recovery_is_auth_failure(s text, message text) RETURNS boolean AS $$
DECLARE body jsonb;
BEGIN
    IF s IS DISTINCT FROM 'error' THEN RETURN false; END IF;
    IF message LIKE 'Token revoked (401):%' OR message LIKE 'Unauthorized (401):%' THEN RETURN true; END IF;
    IF message NOT LIKE 'Authentication failed (401):%' THEN RETURN false; END IF;
    BEGIN body := substr(message,length('Authentication failed (401):')+1)::jsonb;
    EXCEPTION WHEN invalid_text_representation THEN RETURN false; END;
    RETURN coalesce(lower(trim(body->'error'->>'code')) IN
       ('auth_failed','invalid_token','token_invalid','requires_relogin','invalid_grant','revoked',
        'token_invalidated','token_revoked','refresh_token_reused','refresh_token_invalidated'),false);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Only the auth tuple advances the opaque generation; no token/digest is exposed.
CREATE FUNCTION proxy_recovery_source_generation() RETURNS trigger AS $$
BEGIN
    IF NEW.extra->>'proxy_replica_source'='true' THEN
        IF NEW.proxy_credential_generation IS NULL OR (NEW.credentials IS DISTINCT FROM OLD.credentials AND
           jsonb_build_array(NEW.credentials->'access_token',NEW.credentials->'refresh_token',NEW.credentials->'id_token',NEW.credentials->'chatgpt_account_id',NEW.credentials->'chatgpt_user_id',NEW.credentials->'organization_id',NEW.credentials->'auth_mode')
           IS DISTINCT FROM jsonb_build_array(OLD.credentials->'access_token',OLD.credentials->'refresh_token',OLD.credentials->'id_token',OLD.credentials->'chatgpt_account_id',OLD.credentials->'chatgpt_user_id',OLD.credentials->'organization_id',OLD.credentials->'auth_mode')) THEN
            NEW.proxy_credential_generation := gen_random_uuid()::text;
            NEW.proxy_verified_generation := NULL;
            NEW.proxy_verified_until := NULL;
        END IF;
        IF proxy_recovery_is_auth_failure(NEW.status,NEW.error_message) THEN
            NEW.proxy_verified_generation := NULL;
            NEW.proxy_verified_until := NULL;
            IF NOT proxy_recovery_is_auth_failure(OLD.status,OLD.error_message) THEN
                NEW.proxy_auth_fault := gen_random_uuid()::text;
            END IF;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER proxy_recovery_generation BEFORE UPDATE OF credentials,status,error_message,extra ON accounts
FOR EACH ROW EXECUTE FUNCTION proxy_recovery_source_generation();

-- UPDATE OF observes explicit false->false too. Internal owner pauses/resume
-- bypass this trigger only for the exact owner's children in that transaction.
CREATE FUNCTION proxy_recovery_manual_override() RETURNS trigger AS $$
DECLARE owner text; internal text;
BEGIN
    owner := OLD.extra->>'credential_owner_account_id';
    internal := current_setting('sub2api.proxy_recovery_internal',true);
    IF owner IS NOT NULL AND internal IS DISTINCT FROM 'pause:'||owner AND internal IS DISTINCT FROM 'resume:'||owner THEN
        NEW.proxy_recovery_revision := OLD.proxy_recovery_revision+1;
        UPDATE account_proxy_recovery_pauses SET revoked_reason='manual_pause' WHERE account_id=OLD.id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER proxy_recovery_manual BEFORE UPDATE OF schedulable ON accounts
FOR EACH ROW EXECUTE FUNCTION proxy_recovery_manual_override();

CREATE FUNCTION proxy_recovery_config_override() RETURNS trigger AS $$
BEGIN
    IF OLD.extra ? 'credential_owner_account_id' AND
       (NEW.status IS DISTINCT FROM OLD.status OR NEW.deleted_at IS DISTINCT FROM OLD.deleted_at OR
        NEW.proxy_id IS DISTINCT FROM OLD.proxy_id OR NEW.concurrency IS DISTINCT FROM OLD.concurrency OR
        NEW.priority IS DISTINCT FROM OLD.priority OR NEW.expires_at IS DISTINCT FROM OLD.expires_at OR
        NEW.auto_pause_on_expired IS DISTINCT FROM OLD.auto_pause_on_expired OR
        NEW.credentials IS DISTINCT FROM OLD.credentials OR NEW.extra IS DISTINCT FROM OLD.extra OR
        NEW.name IS DISTINCT FROM OLD.name OR NEW.notes IS DISTINCT FROM OLD.notes OR
        NEW.load_factor IS DISTINCT FROM OLD.load_factor OR NEW.rate_multiplier IS DISTINCT FROM OLD.rate_multiplier) THEN
        NEW.proxy_recovery_revision := OLD.proxy_recovery_revision+1;
        UPDATE account_proxy_recovery_pauses SET revoked_reason='config_changed' WHERE account_id=OLD.id;
    END IF;
    IF OLD.extra->>'proxy_replica_source'='true' AND
       (NEW.deleted_at IS DISTINCT FROM OLD.deleted_at OR NEW.expires_at IS DISTINCT FROM OLD.expires_at OR
        NEW.proxy_id IS DISTINCT FROM OLD.proxy_id OR NEW.platform IS DISTINCT FROM OLD.platform OR
        NEW.type IS DISTINCT FROM OLD.type OR NEW.status NOT IN ('active','error')) THEN
        UPDATE account_proxy_recovery_pauses SET revoked_reason='source_unavailable' WHERE source_account_id=OLD.id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER proxy_recovery_config BEFORE UPDATE OF status,deleted_at,proxy_id,concurrency,priority,expires_at,auto_pause_on_expired,credentials,extra,name,notes,load_factor,rate_multiplier,platform,type ON accounts
FOR EACH ROW EXECUTE FUNCTION proxy_recovery_config_override();

-- Association writers must lock the account before changing membership. This
-- also serializes raw/import writes with the recovery's final account locks.
CREATE FUNCTION proxy_recovery_group_override() RETURNS trigger AS $$
DECLARE target bigint;
BEGIN
    IF TG_OP='INSERT' THEN target:=NEW.account_id; ELSE target:=OLD.account_id; END IF;
    UPDATE accounts SET proxy_recovery_revision=proxy_recovery_revision+1 WHERE id=target
      AND (extra ? 'credential_owner_account_id' OR extra->>'proxy_replica_source'='true');
    UPDATE account_proxy_recovery_pauses SET revoked_reason='config_changed'
      WHERE account_id=target OR source_account_id=target;
    IF TG_OP='UPDATE' AND NEW.account_id<>OLD.account_id THEN
        UPDATE accounts SET proxy_recovery_revision=proxy_recovery_revision+1 WHERE id=NEW.account_id
          AND (extra ? 'credential_owner_account_id' OR extra->>'proxy_replica_source'='true');
        UPDATE account_proxy_recovery_pauses SET revoked_reason='config_changed'
          WHERE account_id=NEW.account_id OR source_account_id=NEW.account_id;
    END IF;
    IF TG_OP='DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER proxy_recovery_groups BEFORE INSERT OR UPDATE OR DELETE ON account_groups
FOR EACH ROW EXECUTE FUNCTION proxy_recovery_group_override();

CREATE FUNCTION proxy_recovery_group_config_override() RETURNS trigger AS $$
BEGIN
    IF NEW IS DISTINCT FROM OLD THEN
        PERFORM a.id FROM accounts a JOIN account_groups ag ON ag.account_id=a.id
          WHERE ag.group_id=OLD.id AND (a.extra ? 'credential_owner_account_id' OR a.extra->>'proxy_replica_source'='true')
          ORDER BY a.id FOR UPDATE OF a;
        UPDATE account_proxy_recovery_pauses SET revoked_reason='config_changed'
          WHERE account_id IN(SELECT account_id FROM account_groups WHERE group_id=OLD.id)
             OR source_account_id IN(SELECT account_id FROM account_groups WHERE group_id=OLD.id);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER proxy_recovery_group_config BEFORE UPDATE ON groups
FOR EACH ROW EXECUTE FUNCTION proxy_recovery_group_config_override();

CREATE FUNCTION proxy_recovery_proxy_config_override() RETURNS trigger AS $$
BEGIN
    IF NEW IS DISTINCT FROM OLD THEN
        PERFORM id FROM accounts WHERE proxy_id=OLD.id AND extra ? 'credential_owner_account_id'
          ORDER BY id FOR UPDATE;
        UPDATE account_proxy_recovery_pauses SET revoked_reason='config_changed'
          WHERE account_id IN(SELECT id FROM accounts WHERE proxy_id=OLD.id)
             OR source_account_id IN(SELECT id FROM accounts WHERE proxy_id=OLD.id);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER proxy_recovery_proxy_config BEFORE UPDATE OF protocol,host,port,username,password,status,deleted_at,expires_at ON proxies
FOR EACH ROW EXECUTE FUNCTION proxy_recovery_proxy_config_override();

CREATE FUNCTION proxy_recovery_performance_override() RETURNS trigger AS $$
BEGIN
    IF NEW.paused THEN
        UPDATE accounts SET proxy_recovery_revision=proxy_recovery_revision+1 WHERE id=NEW.account_id;
        UPDATE account_proxy_recovery_pauses SET revoked_reason='config_changed'
          WHERE account_id=NEW.account_id OR source_account_id=NEW.account_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER proxy_recovery_performance BEFORE INSERT OR UPDATE ON account_performance_monitors
FOR EACH ROW EXECUTE FUNCTION proxy_recovery_performance_override();

CREATE OR REPLACE FUNCTION propagate_proxy_replica_owner_change() RETURNS trigger AS $$
DECLARE owner_id bigint; previous_internal text;
BEGIN
    owner_id:=OLD.id;
    IF OLD.extra->>'proxy_replica_source' IS DISTINCT FROM 'true' THEN
        IF TG_OP='DELETE' THEN RETURN OLD; END IF;
        IF NEW.extra->>'proxy_replica_source' IS DISTINCT FROM 'true' THEN RETURN NEW; END IF;
    END IF;
    IF OLD.extra ? 'credential_owner_account_id' THEN
        IF TG_OP='DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP='DELETE' THEN
        UPDATE account_proxy_recovery_pauses SET revoked_reason='source_unavailable' WHERE source_account_id=owner_id;
        UPDATE accounts SET schedulable=false,status='disabled',updated_at=NOW()
          WHERE extra ? 'credential_owner_account_id' AND extra->>'credential_owner_account_id'=owner_id::text AND deleted_at IS NULL;
    ELSIF NEW.deleted_at IS NOT NULL OR NEW.status<>'active' OR
          (NEW.expires_at IS NOT NULL AND NEW.expires_at<=NOW()) THEN
        -- Match manual writers' account->proof lock order.
        PERFORM id FROM accounts WHERE extra ? 'credential_owner_account_id' AND extra->>'credential_owner_account_id'=owner_id::text
          AND deleted_at IS NULL ORDER BY id FOR UPDATE;
        IF proxy_recovery_is_auth_failure(NEW.status,NEW.error_message) AND
           NOT proxy_recovery_is_auth_failure(OLD.status,OLD.error_message) AND
           NEW.deleted_at IS NULL AND (NEW.expires_at IS NULL OR NEW.expires_at>NOW()) THEN
            INSERT INTO account_proxy_recovery_pauses(account_id,source_account_id,fault,credential_generation,revision)
            SELECT a.id,owner_id,NEW.proxy_auth_fault,NEW.proxy_credential_generation,a.proxy_recovery_revision
            FROM accounts a JOIN proxies p ON p.id=a.proxy_id
            WHERE a.extra ? 'credential_owner_account_id' AND a.extra->>'credential_owner_account_id'=owner_id::text AND a.deleted_at IS NULL
              AND a.schedulable AND a.status='active' AND (a.expires_at IS NULL OR a.expires_at>NOW())
              AND (a.rate_limit_reset_at IS NULL OR a.rate_limit_reset_at<=NOW())
              AND (a.overload_until IS NULL OR a.overload_until<=NOW())
              AND (a.temp_unschedulable_until IS NULL OR a.temp_unschedulable_until<=NOW())
              AND NOT EXISTS(SELECT 1 FROM account_performance_monitors m WHERE m.account_id=a.id AND m.paused)
              AND p.deleted_at IS NULL AND p.status='active' AND (p.expires_at IS NULL OR p.expires_at>NOW())
              AND NOT EXISTS(SELECT 1 FROM account_groups ag JOIN groups g ON g.id=ag.group_id
                             WHERE ag.account_id=a.id AND (g.deleted_at IS NOT NULL OR g.status<>'active'))
            ON CONFLICT(account_id) DO UPDATE SET fault=EXCLUDED.fault,credential_generation=EXCLUDED.credential_generation,
                revision=EXCLUDED.revision,revoked_reason='';
        END IF;
        previous_internal:=current_setting('sub2api.proxy_recovery_internal',true);
        PERFORM set_config('sub2api.proxy_recovery_internal','pause:'||owner_id::text,true);
        UPDATE accounts SET schedulable=false,updated_at=NOW()
          WHERE extra ? 'credential_owner_account_id' AND extra->>'credential_owner_account_id'=owner_id::text AND deleted_at IS NULL;
        PERFORM set_config('sub2api.proxy_recovery_internal',coalesce(previous_internal,''),true);
    END IF;
    INSERT INTO scheduler_outbox(event_type,account_id) SELECT 'account_changed',id FROM accounts
      WHERE extra ? 'credential_owner_account_id' AND extra->>'credential_owner_account_id'=owner_id::text;
    IF TG_OP='DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
