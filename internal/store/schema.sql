-- Schema is idempotent and applied at startup under an advisory lock.

CREATE TABLE IF NOT EXISTS shows (
    id             uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    name           text        NOT NULL,
    price_paise    bigint      NOT NULL CHECK (price_paise >= 0),
    per_user_limit integer     NOT NULL CHECK (per_user_limit > 0),
    total_seats    integer     NOT NULL CHECK (total_seats > 0),
    created_at     timestamptz NOT NULL DEFAULT now()
);

-- One row per successful reservation. The (user_id, idempotency_key) unique
-- constraint is the idempotency store: a key can only ever produce one row.
CREATE TABLE IF NOT EXISTS reservations (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    show_id         uuid        NOT NULL REFERENCES shows (id),
    user_id         text        NOT NULL,
    idempotency_key text        NOT NULL,
    request_hash    text        NOT NULL,
    seats           text[]      NOT NULL,
    amount_paise    bigint      NOT NULL CHECK (amount_paise >= 0),
    status          text        NOT NULL CHECK (status IN ('confirmed', 'cancelled')),
    created_at      timestamptz NOT NULL DEFAULT now(),
    cancelled_at    timestamptz,
    UNIQUE (user_id, idempotency_key)
);

-- The seat row is the unit of contention. Its primary key makes a seat a
-- single physical row, so "who owns A12" has exactly one answer.
CREATE TABLE IF NOT EXISTS seats (
    show_id        uuid        NOT NULL REFERENCES shows (id),
    label          text        NOT NULL,
    position       integer     NOT NULL,
    status         text        NOT NULL DEFAULT 'available'
                               CHECK (status IN ('available', 'confirmed')),
    reservation_id uuid        REFERENCES reservations (id),
    user_id        text,
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (show_id, label),
    -- An available seat has no owner; a taken seat always has one.
    CHECK ((status = 'available') = (reservation_id IS NULL)),
    CHECK ((reservation_id IS NULL) = (user_id IS NULL))
);

CREATE INDEX IF NOT EXISTS seats_reservation_idx ON seats (reservation_id)
    WHERE reservation_id IS NOT NULL;

-- Per-user seat counter for a show. Incremented with a guarded upsert, which
-- row-locks the counter, so parallel requests from one user serialise here.
CREATE TABLE IF NOT EXISTS user_show_counts (
    show_id    uuid    NOT NULL REFERENCES shows (id),
    user_id    text    NOT NULL,
    seats_held integer NOT NULL CHECK (seats_held >= 0),
    PRIMARY KEY (show_id, user_id)
);
