-- Seed data for catalog, engagement, and ads entities not covered by the churn CSV
\c spotify_test;

-- Ensure country reference data for the artists below
WITH seed_countries(code, name) AS (
    VALUES
        ('US', 'United States'),
        ('UK', 'United Kingdom'),
        ('KR', 'South Korea')
)
INSERT INTO countries (country_code, country_name)
SELECT sc.code, sc.name
FROM seed_countries sc
WHERE NOT EXISTS (
    SELECT 1 FROM countries c WHERE c.country_code = sc.code
);

-- -------------------------------------------------------------------------------------
-- Synthetic seed data for entities not present in the churn dataset
-- -------------------------------------------------------------------------------------
WITH seed_genres(name) AS (
    VALUES ('Pop'), ('Rock'), ('Hip Hop'), ('Electronic'), ('Indie')
)
INSERT INTO genres (name)
SELECT sg.name
FROM seed_genres sg
WHERE NOT EXISTS (SELECT 1 FROM genres g WHERE g.name = sg.name);

WITH seed_artists(name, country_code, debut_year) AS (
    VALUES
        ('Taylor Swift', 'US', 2006),
        ('Ed Sheeran', 'UK', 2011),
        ('BTS', 'KR', 2013),
        ('Adele', 'UK', 2008),
        ('Billie Eilish', 'US', 2015)
)
INSERT INTO artists (name, country_code, debut_year)
SELECT sa.name, sa.country_code, sa.debut_year
FROM seed_artists sa
WHERE NOT EXISTS (SELECT 1 FROM artists a WHERE a.name = sa.name);

WITH seed_albums(artist_name, title, release_date, label) AS (
    VALUES
        ('Taylor Swift', 'Lover', DATE '2019-08-23', 'Republic Records'),
        ('Ed Sheeran', 'Divide', DATE '2017-03-03', 'Asylum Records'),
        ('BTS', 'Map of the Soul: 7', DATE '2020-02-21', 'Big Hit Entertainment'),
        ('Adele', '30', DATE '2021-11-19', 'Columbia Records'),
        ('Billie Eilish', 'Happier Than Ever', DATE '2021-07-30', 'Darkroom')
)
INSERT INTO albums (artist_id, title, release_date, label)
SELECT ar.artist_id, sa.title, sa.release_date, sa.label
FROM seed_albums sa
JOIN artists ar ON ar.name = sa.artist_name
WHERE NOT EXISTS (
    SELECT 1 FROM albums al
    WHERE al.title = sa.title AND al.artist_id = ar.artist_id
);

WITH seed_tracks(album_title, title, duration_ms, explicit, popularity) AS (
    VALUES
        ('Lover', 'Cruel Summer', 178000, false, 90),
        ('Lover', 'Lover', 221000, false, 85),
        ('Divide', 'Shape of You', 234000, false, 95),
        ('Divide', 'Perfect', 263000, false, 92),
        ('Map of the Soul: 7', 'ON', 298000, false, 88),
        ('Map of the Soul: 7', 'Black Swan', 222000, false, 86),
        ('30', 'Easy On Me', 224000, false, 93),
        ('Happier Than Ever', 'Happier Than Ever', 299000, true, 89)
)
INSERT INTO tracks (album_id, title, duration_ms, explicit, popularity)
SELECT al.album_id, st.title, st.duration_ms, st.explicit, st.popularity
FROM seed_tracks st
JOIN albums al ON al.title = st.album_title
WHERE NOT EXISTS (
    SELECT 1 FROM tracks t
    WHERE t.title = st.title AND t.album_id = al.album_id
);

WITH seed_track_genres(track_title, genre_name) AS (
    VALUES
        ('Cruel Summer', 'Pop'),
        ('Lover', 'Pop'),
        ('Shape of You', 'Pop'),
        ('Perfect', 'Pop'),
        ('ON', 'Hip Hop'),
        ('Black Swan', 'Electronic'),
        ('Easy On Me', 'Indie'),
        ('Happier Than Ever', 'Rock')
)
INSERT INTO track_genres (track_id, genre_id)
SELECT t.track_id, g.genre_id
FROM seed_track_genres stg
JOIN tracks t ON t.title = stg.track_title
JOIN genres g ON g.name = stg.genre_name
WHERE NOT EXISTS (
    SELECT 1 FROM track_genres tg
    WHERE tg.track_id = t.track_id AND tg.genre_id = g.genre_id
);

WITH seed_track_artists(track_title, artist_name, role) AS (
    VALUES
        ('Cruel Summer', 'Taylor Swift', 'primary'),
        ('Lover', 'Taylor Swift', 'primary'),
        ('Shape of You', 'Ed Sheeran', 'primary'),
        ('Perfect', 'Ed Sheeran', 'primary'),
        ('ON', 'BTS', 'primary'),
        ('Black Swan', 'BTS', 'primary'),
        ('Easy On Me', 'Adele', 'primary'),
        ('Happier Than Ever', 'Billie Eilish', 'primary')
)
INSERT INTO track_artists (track_id, artist_id, role)
SELECT t.track_id, a.artist_id, sta.role
FROM seed_track_artists sta
JOIN tracks t ON t.title = sta.track_title
JOIN artists a ON a.name = sta.artist_name
WHERE NOT EXISTS (
    SELECT 1 FROM track_artists ta
    WHERE ta.track_id = t.track_id AND ta.artist_id = a.artist_id
);

WITH ranked_users AS (
    SELECT user_id,
           ROW_NUMBER() OVER (ORDER BY user_id) AS rn
    FROM users
),
seed_playlists(rn, name, description, is_public) AS (
    VALUES
        (1, 'Morning Boost', 'Upbeat tracks for early productivity', true),
        (2, 'Deep Focus', 'Instrumental tracks for studying', false),
        (3, 'Weekend Drive', 'Feel-good songs for the road', true)
)
INSERT INTO playlists (owner_id, name, description, is_public)
SELECT ru.user_id, sp.name, sp.description, sp.is_public
FROM seed_playlists sp
JOIN ranked_users ru ON ru.rn = sp.rn
WHERE NOT EXISTS (
    SELECT 1 FROM playlists p
    WHERE p.owner_id = ru.user_id AND p.name = sp.name
);

WITH ranked_users AS (
    SELECT user_id,
           ROW_NUMBER() OVER (ORDER BY user_id) AS rn
    FROM users
    ORDER BY user_id
    LIMIT 5
),
ranked_devices AS (
    SELECT device_id,
           ROW_NUMBER() OVER (ORDER BY device_id) AS rn
    FROM devices
    ORDER BY device_id
    LIMIT 5
),
user_device_pairs AS (
    SELECT ru.user_id, rd.device_id
    FROM ranked_users ru
    JOIN ranked_devices rd ON rd.rn = ru.rn
)
INSERT INTO user_devices (user_id, device_id)
SELECT udp.user_id, udp.device_id
FROM user_device_pairs udp
WHERE NOT EXISTS (
    SELECT 1 FROM user_devices ud
    WHERE ud.user_id = udp.user_id AND ud.device_id = udp.device_id
);

WITH ranked_playlists AS (
    SELECT playlist_id,
           ROW_NUMBER() OVER (ORDER BY playlist_id) AS rn
    FROM playlists
    WHERE name IN ('Morning Boost', 'Deep Focus', 'Weekend Drive')
),
ranked_tracks AS (
    SELECT track_id,
           ROW_NUMBER() OVER (ORDER BY track_id) AS rn
    FROM tracks
    WHERE title IN (
        'Cruel Summer',
        'Lover',
        'Shape of You',
        'Perfect',
        'ON',
        'Happier Than Ever'
    )
),
seed_playlist_tracks(playlist_rn, track_rn, position) AS (
    VALUES
        (1, 1, 1),
        (1, 2, 2),
        (2, 3, 1),
        (2, 4, 2),
        (3, 5, 1),
        (3, 6, 2)
)
INSERT INTO playlist_tracks (playlist_id, track_id, position)
SELECT rp.playlist_id, rt.track_id, spt.position
FROM seed_playlist_tracks spt
JOIN ranked_playlists rp ON rp.rn = spt.playlist_rn
JOIN ranked_tracks rt ON rt.rn = spt.track_rn
WHERE NOT EXISTS (
    SELECT 1 FROM playlist_tracks pt
    WHERE pt.playlist_id = rp.playlist_id AND pt.track_id = rt.track_id
);

WITH session_source AS (
    SELECT u.user_id,
           COALESCE(u.preferred_device_id, d.device_id) AS device_id,
           NOW() - (ROW_NUMBER() OVER (ORDER BY u.user_id) * INTERVAL '1 day') AS start_time,
           NOW() - (ROW_NUMBER() OVER (ORDER BY u.user_id) * INTERVAL '1 day') + INTERVAL '45 minutes' AS end_time
    FROM users u
    LEFT JOIN devices d ON d.device_type = 'Desktop'
    ORDER BY u.user_id
    LIMIT 5
)
INSERT INTO listening_sessions (user_id, device_id, started_at, ended_at, total_minutes, network_type)
SELECT ss.user_id,
       ss.device_id,
       ss.start_time,
       ss.end_time,
       45,
       'wifi'
FROM session_source ss
WHERE NOT EXISTS (
    SELECT 1 FROM listening_sessions ls
    WHERE ls.user_id = ss.user_id AND ls.started_at = ss.start_time
);

WITH ranked_sessions AS (
    SELECT session_id,
           ROW_NUMBER() OVER (ORDER BY session_id) AS rn
    FROM listening_sessions
    ORDER BY session_id
    LIMIT 5
),
seed_session_tracks(session_rn, track_title, played_ms, skipped) AS (
    VALUES
        (1, 'Cruel Summer', 170000, false),
        (1, 'Lover', 200000, true),
        (2, 'Shape of You', 220000, false),
        (3, 'Perfect', 240000, false),
        (4, 'ON', 280000, false),
        (5, 'Happier Than Ever', 260000, false)
)
INSERT INTO session_tracks (session_id, track_id, played_ms, skipped)
SELECT rs.session_id,
       t.track_id,
       sst.played_ms,
       sst.skipped
FROM seed_session_tracks sst
JOIN ranked_sessions rs ON rs.rn = sst.session_rn
JOIN tracks t ON t.title = sst.track_title
WHERE NOT EXISTS (
    SELECT 1 FROM session_tracks st
    WHERE st.session_id = rs.session_id AND st.track_id = t.track_id
);

WITH seed_ads(advertiser, ad_type, cpm_usd) AS (
    VALUES
        ('FreshBrew Coffee', 'audio', 12.50),
        ('Urban Wheels', 'audio', 18.75),
        ('GlowFit Wearables', 'video', 22.10)
)
INSERT INTO ad_campaigns (advertiser, ad_type, cpm_usd)
SELECT sa.advertiser, sa.ad_type, sa.cpm_usd
FROM seed_ads sa
WHERE NOT EXISTS (
    SELECT 1 FROM ad_campaigns ac WHERE ac.advertiser = sa.advertiser
);

WITH sessions_sample AS (
    SELECT session_id,
           user_id,
           ROW_NUMBER() OVER (ORDER BY session_id) AS rn
    FROM listening_sessions
    ORDER BY session_id
    LIMIT 5
),
ads_sample AS (
    SELECT ad_id,
           ROW_NUMBER() OVER (ORDER BY ad_id) AS rn
    FROM ad_campaigns
    ORDER BY ad_id
    LIMIT 3
),
seed_impressions(session_rn, ad_rn, completed) AS (
    VALUES
        (1, 1, true),
        (2, 2, false),
        (3, 3, true),
        (4, 1, true),
        (5, 2, false)
)
INSERT INTO ad_impressions (ad_id, user_id, session_id, occurred_at, completed)
SELECT ads.ad_id,
       ss.user_id,
       ss.session_id,
       NOW() - (ss.rn * INTERVAL '5 minutes'),
       si.completed
FROM seed_impressions si
JOIN sessions_sample ss ON ss.rn = si.session_rn
JOIN ads_sample ads ON ads.rn = si.ad_rn
WHERE NOT EXISTS (
    SELECT 1 FROM ad_impressions ai
    WHERE ai.session_id = ss.session_id AND ai.ad_id = ads.ad_id
);

