CREATE DATABASE spotify_test;

\c spotify_test;

-- -------------------------------------------------------------------------------------
-- Core reference tables
-- -------------------------------------------------------------------------------------
CREATE TABLE subscription_plans (
    plan_id          SERIAL PRIMARY KEY,
    name             TEXT UNIQUE NOT NULL,
    max_devices      INTEGER NOT NULL DEFAULT 1,
    allows_downloads BOOLEAN NOT NULL DEFAULT false,
    price_usd        NUMERIC(6,2) NOT NULL CHECK (price_usd >= 0),
    created_at       TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE countries (
    country_code CHAR(2) PRIMARY KEY,
    country_name TEXT NOT NULL
);

CREATE TABLE devices (
    device_id   SERIAL PRIMARY KEY,
    device_type TEXT NOT NULL,
    UNIQUE (device_type)
);

CREATE TABLE genres (
    genre_id SERIAL PRIMARY KEY,
    name     TEXT UNIQUE NOT NULL
);

-- -------------------------------------------------------------------------------------
-- User-centric tables
-- -------------------------------------------------------------------------------------
CREATE TABLE users (
    user_id        INTEGER PRIMARY KEY,
    gender         TEXT NOT NULL,
    age            INTEGER NOT NULL CHECK (age BETWEEN 13 AND 120),
    country_code   CHAR(2) NOT NULL REFERENCES countries (country_code),
    preferred_device_id INTEGER REFERENCES devices (device_id),
    created_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE user_subscriptions (
    user_subscription_id SERIAL PRIMARY KEY,
    user_id              INTEGER NOT NULL REFERENCES users (user_id) ON DELETE CASCADE,
    plan_id              INTEGER NOT NULL REFERENCES subscription_plans (plan_id),
    is_current           BOOLEAN NOT NULL DEFAULT true,
    started_at           TIMESTAMP NOT NULL DEFAULT NOW(),
    ended_at             TIMESTAMP
);

CREATE TABLE user_metrics (
    metric_id              SERIAL PRIMARY KEY,
    user_id                INTEGER NOT NULL REFERENCES users (user_id) ON DELETE CASCADE,
    listening_time_minutes INTEGER NOT NULL,
    songs_played_per_day   INTEGER NOT NULL,
    skip_rate              NUMERIC(5,2) NOT NULL CHECK (skip_rate BETWEEN 0 AND 100),
    ads_listened_per_week  INTEGER NOT NULL,
    offline_listening      BOOLEAN NOT NULL,
    device_type_reported   TEXT NOT NULL,
    is_churned             BOOLEAN NOT NULL,
    snapshot_date          DATE NOT NULL DEFAULT CURRENT_DATE
);

CREATE TABLE user_devices (
    user_id   INTEGER NOT NULL REFERENCES users (user_id) ON DELETE CASCADE,
    device_id INTEGER NOT NULL REFERENCES devices (device_id),
    PRIMARY KEY (user_id, device_id)
);

-- -------------------------------------------------------------------------------------
-- Music catalog tables
-- -------------------------------------------------------------------------------------
CREATE TABLE artists (
    artist_id SERIAL PRIMARY KEY,
    name      TEXT NOT NULL,
    country_code CHAR(2) REFERENCES countries (country_code),
    debut_year INTEGER
);

CREATE TABLE albums (
    album_id   SERIAL PRIMARY KEY,
    artist_id  INTEGER NOT NULL REFERENCES artists (artist_id) ON DELETE CASCADE,
    title      TEXT NOT NULL,
    release_date DATE,
    label      TEXT
);

CREATE TABLE tracks (
    track_id    SERIAL PRIMARY KEY,
    album_id    INTEGER REFERENCES albums (album_id) ON DELETE SET NULL,
    title       TEXT NOT NULL,
    duration_ms INTEGER NOT NULL CHECK (duration_ms > 0),
    explicit    BOOLEAN NOT NULL DEFAULT false,
    popularity  INTEGER CHECK (popularity BETWEEN 0 AND 100)
);

CREATE TABLE track_artists (
    track_id  INTEGER NOT NULL REFERENCES tracks (track_id) ON DELETE CASCADE,
    artist_id INTEGER NOT NULL REFERENCES artists (artist_id) ON DELETE CASCADE,
    role      TEXT NOT NULL DEFAULT 'primary',
    PRIMARY KEY (track_id, artist_id)
);

CREATE TABLE track_genres (
    track_id INTEGER NOT NULL REFERENCES tracks (track_id) ON DELETE CASCADE,
    genre_id INTEGER NOT NULL REFERENCES genres (genre_id) ON DELETE CASCADE,
    PRIMARY KEY (track_id, genre_id)
);

-- -------------------------------------------------------------------------------------
-- Engagement tables
-- -------------------------------------------------------------------------------------
CREATE TABLE playlists (
    playlist_id SERIAL PRIMARY KEY,
    owner_id    INTEGER NOT NULL REFERENCES users (user_id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    description TEXT,
    is_public   BOOLEAN NOT NULL DEFAULT false,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE playlist_tracks (
    playlist_id INTEGER NOT NULL REFERENCES playlists (playlist_id) ON DELETE CASCADE,
    track_id    INTEGER NOT NULL REFERENCES tracks (track_id),
    position    INTEGER NOT NULL,
    added_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (playlist_id, track_id)
);

CREATE TABLE listening_sessions (
    session_id  SERIAL PRIMARY KEY,
    user_id     INTEGER NOT NULL REFERENCES users (user_id) ON DELETE CASCADE,
    device_id   INTEGER REFERENCES devices (device_id),
    started_at  TIMESTAMP NOT NULL,
    ended_at    TIMESTAMP,
    total_minutes INTEGER CHECK (total_minutes >= 0),
    network_type TEXT
);

CREATE TABLE session_tracks (
    session_id INTEGER NOT NULL REFERENCES listening_sessions (session_id) ON DELETE CASCADE,
    track_id   INTEGER NOT NULL REFERENCES tracks (track_id),
    played_ms  INTEGER NOT NULL CHECK (played_ms >= 0),
    skipped    BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (session_id, track_id)
);

CREATE TABLE ad_campaigns (
    ad_id       SERIAL PRIMARY KEY,
    advertiser  TEXT NOT NULL,
    ad_type     TEXT NOT NULL,
    cpm_usd     NUMERIC(6,2) NOT NULL CHECK (cpm_usd >= 0),
    is_active   BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE ad_impressions (
    impression_id SERIAL PRIMARY KEY,
    ad_id         INTEGER NOT NULL REFERENCES ad_campaigns (ad_id),
    user_id       INTEGER REFERENCES users (user_id) ON DELETE SET NULL,
    session_id    INTEGER REFERENCES listening_sessions (session_id) ON DELETE SET NULL,
    occurred_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    completed     BOOLEAN NOT NULL DEFAULT false
);

-- -------------------------------------------------------------------------------------
-- Data ingestion from the churn dataset
-- -------------------------------------------------------------------------------------
CREATE TABLE staging_user_metrics (
    user_id INTEGER,
    gender TEXT,
    age INTEGER,
    country TEXT,
    subscription_type TEXT,
    listening_time INTEGER,
    songs_played_per_day INTEGER,
    skip_rate NUMERIC(5,2),
    device_type TEXT,
    ads_listened_per_week INTEGER,
    offline_listening BOOLEAN,
    is_churned BOOLEAN
);

COPY staging_user_metrics (
    user_id,
    gender,
    age,
    country,
    subscription_type,
    listening_time,
    songs_played_per_day,
    skip_rate,
    device_type,
    ads_listened_per_week,
    offline_listening,
    is_churned
)
FROM 'D:/codeproject/assignments/abd/tubes/spotify_churn_dataset.csv'
WITH (
    FORMAT csv,
    HEADER true,
    DELIMITER ',',
    QUOTE '"',
    NULL ''
);

-- Seed lookup tables from staging data
INSERT INTO countries (country_code, country_name)
SELECT DISTINCT UPPER(LEFT(country, 2)), country
FROM staging_user_metrics
ON CONFLICT (country_code) DO NOTHING;

INSERT INTO devices (device_type, os_family)
SELECT DISTINCT device_type, 'unknown'
FROM staging_user_metrics
ON CONFLICT (device_type, os_family) DO NOTHING;

INSERT INTO subscription_plans (name, max_devices, allows_downloads, price_usd)
SELECT DISTINCT subscription_type, 3, true, 0
FROM staging_user_metrics
ON CONFLICT (name) DO NOTHING;

-- Populate users
INSERT INTO users (user_id, gender, age, country_code, preferred_device_id)
SELECT s.user_id,
       s.gender,
       s.age,
       UPPER(LEFT(s.country, 2)),
       d.device_id
FROM staging_user_metrics s
LEFT JOIN devices d ON d.device_type = s.device_type
ON CONFLICT (user_id) DO NOTHING;

-- Attach subscription details
INSERT INTO user_subscriptions (user_id, plan_id, is_current)
SELECT s.user_id, p.plan_id, NOT s.is_churned
FROM staging_user_metrics s
JOIN subscription_plans p ON p.name = s.subscription_type
ON CONFLICT DO NOTHING;

-- Persist user metric snapshots
INSERT INTO user_metrics (
    user_id,
    listening_time_minutes,
    songs_played_per_day,
    skip_rate,
    ads_listened_per_week,
    offline_listening,
    device_type_reported,
    is_churned
)
SELECT user_id,
       listening_time,
       songs_played_per_day,
       skip_rate,
       ads_listened_per_week,
       offline_listening,
       device_type,
       is_churned
FROM staging_user_metrics;
