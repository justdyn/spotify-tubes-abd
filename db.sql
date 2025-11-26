\c spotify_test;

CREATE TABLE users (
    user_id INTEGER PRIMARY KEY,
    gender TEXT NOT NULL,
    age INTEGER NOT NULL,
    country TEXT NOT NULL,
    subscription_type TEXT NOT NULL,
    listening_time INTEGER NOT NULL,
    songs_played_per_day INTEGER NOT NULL,
    skip_rate NUMERIC(5,2) NOT NULL,
    device_type TEXT NOT NULL,
    ads_listened_per_week INTEGER NOT NULL,
    offline_listening BOOLEAN NOT NULL,
    is_churned BOOLEAN NOT NULL,
    imported_at TIMESTAMP DEFAULT NOW()
);

COPY users (
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