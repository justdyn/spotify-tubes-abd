-- ============================================================================
-- DQL (Data Query Language) - European Football Database
-- File: bola-dql.sql
-- Database: laliga_europe
-- Purpose: Kumpulan query untuk live coding dan pembelajaran
-- Author: Senior Database Engineer (20+ years experience)
-- Language: Indonesian (Bahasa Indonesia)
-- ============================================================================

-- ============================================================================
-- BAGIAN 1: QUERY DASAR (BASIC QUERIES)
-- ============================================================================

-- Query 1.1: Menampilkan semua liga yang aktif
-- Penjelasan: Query sederhana untuk menampilkan semua liga yang masih aktif
-- menggunakan filter WHERE dengan kondisi boolean
SELECT 
    league_id,
    name AS nama_liga,
    country AS negara,
    is_active AS aktif
FROM leagues
WHERE is_active = true
ORDER BY country, name;

-- Query 1.2: Menampilkan semua tim beserta liga mereka
-- Penjelasan: Menggunakan INNER JOIN untuk menggabungkan tabel teams dan leagues
-- INNER JOIN hanya menampilkan baris yang memiliki relasi di kedua tabel
SELECT 
    t.team_id,
    t.name AS nama_tim,
    l.name AS nama_liga,
    l.country AS negara
FROM teams t
INNER JOIN leagues l ON t.league_id = l.league_id
WHERE t.is_active = true
ORDER BY l.country, l.name, t.name;

-- Query 1.3: Menghitung jumlah tim per liga
-- Penjelasan: Menggunakan GROUP BY dan COUNT() untuk agregasi data
-- GROUP BY mengelompokkan data berdasarkan kolom tertentu
SELECT 
    l.name AS nama_liga,
    l.country AS negara,
    COUNT(t.team_id) AS jumlah_tim
FROM leagues l
LEFT JOIN teams t ON l.league_id = t.league_id AND t.is_active = true
GROUP BY l.league_id, l.name, l.country
ORDER BY jumlah_tim DESC, l.name;

-- Query 1.4: Menampilkan 10 pemain teratas berdasarkan total gol
-- Penjelasan: Menggunakan agregasi SUM(), GROUP BY, dan ORDER BY dengan LIMIT
-- SUM() menjumlahkan nilai dari kolom tertentu
SELECT 
    p.player_id,
    p.name AS nama_pemain,
    SUM(a.goals) AS total_gol,
    COUNT(DISTINCT a.game_id) AS jumlah_pertandingan
FROM players p
INNER JOIN appearances a ON p.player_id = a.player_id
WHERE p.is_active = true
GROUP BY p.player_id, p.name
HAVING SUM(a.goals) > 0  -- HAVING digunakan untuk filter hasil agregasi
ORDER BY total_gol DESC
LIMIT 10;

-- ============================================================================
-- BAGIAN 2: QUERY DENGAN JOIN KOMPLEKS
-- ============================================================================

-- Query 2.1: Detail lengkap pertandingan dengan nama tim dan liga
-- Penjelasan: Multiple JOIN untuk menggabungkan 4 tabel (games, leagues, teams 2x)
-- Menggunakan alias untuk tabel yang sama (ht untuk home team, at untuk away team)
SELECT 
    g.game_id,
    g.season AS musim,
    g.date AS tanggal,
    l.name AS liga,
    ht.name AS tim_tuan_rumah,
    at.name AS tim_tamu,
    g.home_goals AS gol_tuan_rumah,
    g.away_goals AS gol_tamu,
    CASE 
        WHEN g.home_goals > g.away_goals THEN ht.name
        WHEN g.away_goals > g.home_goals THEN at.name
        ELSE 'Seri'
    END AS pemenang
FROM games g
INNER JOIN leagues l ON g.league_id = l.league_id
INNER JOIN teams ht ON g.home_team_id = ht.team_id
INNER JOIN teams at ON g.away_team_id = at.team_id
WHERE g.status = 'completed'
ORDER BY g.date DESC
LIMIT 20;

-- Query 2.2: Statistik tim per musim dengan performa
-- Penjelasan: LEFT JOIN untuk memastikan semua tim ditampilkan meskipun belum bermain
-- Menggunakan agregasi kompleks dengan CASE WHEN untuk menghitung win/draw/loss
SELECT 
    t.team_id,
    t.name AS nama_tim,
    l.name AS liga,
    g.season AS musim,
    COUNT(DISTINCT ts.game_id) AS jumlah_pertandingan,
    SUM(CASE WHEN ts.result = 'win' THEN 1 ELSE 0 END) AS menang,
    SUM(CASE WHEN ts.result = 'draw' THEN 1 ELSE 0 END) AS seri,
    SUM(CASE WHEN ts.result = 'loss' THEN 1 ELSE 0 END) AS kalah,
    SUM(ts.goals) AS gol_memasukkan,
    SUM(CASE 
        WHEN ts.location = 'home' THEN g.away_goals
        ELSE g.home_goals
    END) AS gol_kemasukan,
    SUM(ts.goals) - SUM(CASE 
        WHEN ts.location = 'home' THEN g.away_goals
        ELSE g.home_goals
    END) AS selisih_gol,
    SUM(CASE 
        WHEN ts.result = 'win' THEN 3 
        WHEN ts.result = 'draw' THEN 1 
        ELSE 0 
    END) AS poin
FROM teams t
INNER JOIN leagues l ON t.league_id = l.league_id
LEFT JOIN team_stats ts ON t.team_id = ts.team_id
LEFT JOIN games g ON ts.game_id = g.game_id AND g.status = 'completed'
WHERE t.is_active = true
GROUP BY t.team_id, t.name, l.name, g.season
HAVING COUNT(DISTINCT ts.game_id) > 0
ORDER BY g.season DESC, poin DESC, selisih_gol DESC;

-- Query 2.3: Riwayat transfer pemain
-- Penjelasan: Menggunakan bridge table team_players untuk menampilkan riwayat transfer
-- LEFT JOIN memastikan pemain tanpa tim tetap ditampilkan
SELECT 
    p.player_id,
    p.name AS nama_pemain,
    t.name AS nama_tim,
    l.name AS liga,
    tp.season_start AS musim_mulai,
    tp.season_end AS musim_selesai,
    CASE 
        WHEN tp.is_current = true THEN 'Aktif'
        ELSE 'Tidak Aktif'
    END AS status
FROM players p
LEFT JOIN team_players tp ON p.player_id = tp.player_id
LEFT JOIN teams t ON tp.team_id = t.team_id
LEFT JOIN leagues l ON t.league_id = l.league_id
WHERE p.is_active = true
ORDER BY p.name, tp.season_start DESC;

-- ============================================================================
-- BAGIAN 3: SUBQUERY DAN CTE (Common Table Expressions)
-- ============================================================================

-- Query 3.1: Tim dengan performa terbaik menggunakan subquery
-- Penjelasan: Subquery di SELECT untuk menghitung rata-rata poin per musim
-- Subquery adalah query yang berada di dalam query lain
SELECT 
    t.name AS nama_tim,
    l.name AS liga,
    COUNT(DISTINCT g.season) AS jumlah_musim,
    SUM(CASE 
        WHEN ts.result = 'win' THEN 3 
        WHEN ts.result = 'draw' THEN 1 
        ELSE 0 
    END) AS total_poin,
    ROUND(
        SUM(CASE 
            WHEN ts.result = 'win' THEN 3 
            WHEN ts.result = 'draw' THEN 1 
            ELSE 0 
        END)::numeric / NULLIF(COUNT(DISTINCT g.season), 0), 
        2
    ) AS rata_rata_poin_per_musim
FROM teams t
INNER JOIN leagues l ON t.league_id = l.league_id
INNER JOIN team_stats ts ON t.team_id = ts.team_id
INNER JOIN games g ON ts.game_id = g.game_id AND g.status = 'completed'
WHERE t.is_active = true
GROUP BY t.team_id, t.name, l.name
HAVING COUNT(DISTINCT g.season) >= 2
ORDER BY rata_rata_poin_per_musim DESC
LIMIT 10;

-- Query 3.2: Pemain dengan performa terbaik menggunakan CTE
-- Penjelasan: CTE (WITH clause) membuat query lebih readable dan dapat digunakan
-- berkali-kali dalam query yang sama
WITH player_stats AS (
    SELECT 
        p.player_id,
        p.name AS nama_pemain,
        SUM(a.goals) AS total_gol,
        SUM(a.assists) AS total_assist,
        SUM(a.time_played) AS total_menit,
        COUNT(DISTINCT a.game_id) AS jumlah_pertandingan
    FROM players p
    INNER JOIN appearances a ON p.player_id = a.player_id
    INNER JOIN games g ON a.game_id = g.game_id
    WHERE p.is_active = true 
      AND g.status = 'completed'
    GROUP BY p.player_id, p.name
    HAVING SUM(a.time_played) > 0
)
SELECT 
    nama_pemain,
    total_gol,
    total_assist,
    jumlah_pertandingan,
    total_menit,
    ROUND((total_gol::numeric / NULLIF(total_menit, 0) * 90), 2) AS gol_per_90_menit,
    ROUND((total_assist::numeric / NULLIF(total_menit, 0) * 90), 2) AS assist_per_90_menit
FROM player_stats
WHERE total_gol > 0 OR total_assist > 0
ORDER BY (total_gol + total_assist) DESC
LIMIT 20;

-- Query 3.3: Subquery untuk menemukan pemain dengan xG tertinggi
-- Penjelasan: Subquery di WHERE untuk membandingkan dengan rata-rata
-- Menggunakan correlated subquery untuk perbandingan
SELECT 
    p.name AS nama_pemain,
    a.game_id,
    a.x_goals AS expected_goals,
    a.goals AS gol_aktual,
    (a.goals - COALESCE(a.x_goals, 0)) AS selisih_gol_vs_xg
FROM appearances a
INNER JOIN players p ON a.player_id = p.player_id
INNER JOIN games g ON a.game_id = g.game_id
WHERE a.x_goals IS NOT NULL
  AND g.status = 'completed'
  AND a.x_goals > (
      -- Subquery untuk mendapatkan rata-rata xG
      SELECT AVG(x_goals)
      FROM appearances
      WHERE x_goals IS NOT NULL
  )
ORDER BY a.x_goals DESC
LIMIT 15;

-- ============================================================================
-- BAGIAN 4: WINDOW FUNCTIONS
-- ============================================================================

-- Query 4.1: Ranking pemain berdasarkan gol menggunakan ROW_NUMBER()
-- Penjelasan: Window function ROW_NUMBER() memberikan nomor urut untuk setiap baris
-- PARTITION BY mengelompokkan data, ORDER BY menentukan urutan
SELECT 
    nama_pemain,
    liga,
    musim,
    total_gol,
    ROW_NUMBER() OVER (
        PARTITION BY musim, liga 
        ORDER BY total_gol DESC
    ) AS peringkat_di_liga
FROM (
    SELECT 
        p.name AS nama_pemain,
        l.name AS liga,
        g.season AS musim,
        SUM(a.goals) AS total_gol
    FROM players p
    INNER JOIN appearances a ON p.player_id = a.player_id
    INNER JOIN games g ON a.game_id = g.game_id
    INNER JOIN leagues l ON g.league_id = l.league_id
    WHERE g.status = 'completed'
    GROUP BY p.player_id, p.name, l.name, g.season
    HAVING SUM(a.goals) > 0
) AS player_goals
ORDER BY musim DESC, liga, peringkat_di_liga;

-- Query 4.2: Perbandingan performa tim dengan rata-rata liga menggunakan AVG() OVER()
-- Penjelasan: Window function AVG() OVER() menghitung rata-rata tanpa GROUP BY
-- Membandingkan performa tim dengan rata-rata liga di musim yang sama
SELECT 
    t.name AS nama_tim,
    l.name AS liga,
    g.season AS musim,
    SUM(CASE 
        WHEN ts.result = 'win' THEN 3 
        WHEN ts.result = 'draw' THEN 1 
        ELSE 0 
    END) AS poin_tim,
    ROUND(AVG(SUM(CASE 
        WHEN ts.result = 'win' THEN 3 
        WHEN ts.result = 'draw' THEN 1 
        ELSE 0 
    END)) OVER (PARTITION BY l.league_id, g.season), 2) AS rata_rata_poin_liga,
    SUM(CASE 
        WHEN ts.result = 'win' THEN 3 
        WHEN ts.result = 'draw' THEN 1 
        ELSE 0 
    END) - 
    ROUND(AVG(SUM(CASE 
        WHEN ts.result = 'win' THEN 3 
        WHEN ts.result = 'draw' THEN 1 
        ELSE 0 
    END)) OVER (PARTITION BY l.league_id, g.season), 2) AS selisih_dari_rata_rata
FROM teams t
INNER JOIN leagues l ON t.league_id = l.league_id
INNER JOIN team_stats ts ON t.team_id = ts.team_id
INNER JOIN games g ON ts.game_id = g.game_id
WHERE g.status = 'completed'
GROUP BY t.team_id, t.name, l.league_id, l.name, g.season
ORDER BY g.season DESC, l.name, poin_tim DESC;

-- Query 4.3: Running total gol pemain menggunakan SUM() OVER()
-- Penjelasan: Window function SUM() OVER() dengan ORDER BY untuk running total
-- Menampilkan akumulasi gol pemain seiring waktu
SELECT 
    p.name AS nama_pemain,
    g.date AS tanggal_pertandingan,
    g.season AS musim,
    a.goals AS gol_di_pertandingan,
    SUM(a.goals) OVER (
        PARTITION BY p.player_id, g.season 
        ORDER BY g.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total_gol_musim,
    SUM(a.goals) OVER (
        PARTITION BY p.player_id 
        ORDER BY g.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total_gol_karier
FROM players p
INNER JOIN appearances a ON p.player_id = a.player_id
INNER JOIN games g ON a.game_id = g.game_id
WHERE g.status = 'completed' AND a.goals > 0
ORDER BY p.name, g.date;

-- Query 4.4: Perbandingan performa tim dengan tim terbaik menggunakan RANK()
-- Penjelasan: RANK() memberikan ranking dengan handling tie (nilai sama)
-- DENSE_RANK() juga bisa digunakan untuk ranking tanpa gap
SELECT 
    musim,
    liga,
    nama_tim,
    poin,
    selisih_gol,
    RANK() OVER (
        PARTITION BY musim, liga 
        ORDER BY poin DESC, selisih_gol DESC
    ) AS peringkat
FROM (
    SELECT 
        g.season AS musim,
        l.name AS liga,
        t.name AS nama_tim,
        SUM(CASE 
            WHEN ts.result = 'win' THEN 3 
            WHEN ts.result = 'draw' THEN 1 
            ELSE 0 
        END) AS poin,
        SUM(ts.goals) - SUM(CASE 
            WHEN ts.location = 'home' THEN g.away_goals
            ELSE g.home_goals
        END) AS selisih_gol
    FROM teams t
    INNER JOIN leagues l ON t.league_id = l.league_id
    INNER JOIN team_stats ts ON t.team_id = ts.team_id
    INNER JOIN games g ON ts.game_id = g.game_id
    WHERE g.status = 'completed'
    GROUP BY g.season, l.league_id, l.name, t.team_id, t.name
) AS standings
ORDER BY musim DESC, liga, peringkat;

-- ============================================================================
-- BAGIAN 5: ANALYTICAL QUERIES (QUERY ANALITIK)
-- ============================================================================

-- Query 5.1: Analisis efisiensi finishing (gol vs xG)
-- Penjelasan: Membandingkan gol aktual dengan expected goals untuk melihat
-- efisiensi finishing tim atau pemain
SELECT 
    t.name AS nama_tim,
    l.name AS liga,
    SUM(ts.goals) AS total_gol,
    ROUND(SUM(ts.x_goals)::numeric, 2) AS total_xg,
    ROUND((SUM(ts.goals)::numeric / NULLIF(SUM(ts.x_goals), 0)), 2) AS rasio_gol_per_xg,
    SUM(ts.goals) - ROUND(SUM(ts.x_goals)::numeric, 2) AS selisih_gol_minus_xg
FROM teams t
INNER JOIN leagues l ON t.league_id = l.league_id
INNER JOIN team_stats ts ON t.team_id = ts.team_id
INNER JOIN games g ON ts.game_id = g.game_id
WHERE g.status = 'completed' 
  AND ts.x_goals IS NOT NULL
GROUP BY t.team_id, t.name, l.name
HAVING SUM(ts.x_goals) > 0
ORDER BY rasio_gol_per_xg DESC
LIMIT 15;

-- Query 5.2: Analisis akurasi tembakan tim
-- Penjelasan: Menghitung persentase tembakan yang tepat sasaran
SELECT 
    t.name AS nama_tim,
    l.name AS liga,
    SUM(ts.shots) AS total_tembakan,
    SUM(ts.shots_on_target) AS tembakan_tepat_sasaran,
    ROUND(
        (SUM(ts.shots_on_target)::numeric / NULLIF(SUM(ts.shots), 0) * 100), 
        2
    ) AS persentase_akurasi,
    ROUND(
        (SUM(ts.goals)::numeric / NULLIF(SUM(ts.shots_on_target), 0) * 100), 
        2
    ) AS persentase_konversi_gol
FROM teams t
INNER JOIN leagues l ON t.league_id = l.league_id
INNER JOIN team_stats ts ON t.team_id = ts.team_id
INNER JOIN games g ON ts.game_id = g.game_id
WHERE g.status = 'completed'
  AND ts.shots > 0
GROUP BY t.team_id, t.name, l.name
HAVING SUM(ts.shots) >= 100  -- Minimal 100 tembakan untuk validitas statistik
ORDER BY persentase_akurasi DESC;

-- Query 5.3: Analisis performa home vs away
-- Penjelasan: Membandingkan performa tim saat bermain di kandang vs tandang
SELECT 
    t.name AS nama_tim,
    l.name AS liga,
    COUNT(CASE WHEN ts.location = 'home' THEN 1 END) AS pertandingan_home,
    SUM(CASE WHEN ts.location = 'home' AND ts.result = 'win' THEN 3 
             WHEN ts.location = 'home' AND ts.result = 'draw' THEN 1 
             ELSE 0 END) AS poin_home,
    ROUND(
        AVG(CASE WHEN ts.location = 'home' THEN ts.goals END)::numeric, 
        2
    ) AS rata_rata_gol_home,
    COUNT(CASE WHEN ts.location = 'away' THEN 1 END) AS pertandingan_away,
    SUM(CASE WHEN ts.location = 'away' AND ts.result = 'win' THEN 3 
             WHEN ts.location = 'away' AND ts.result = 'draw' THEN 1 
             ELSE 0 END) AS poin_away,
    ROUND(
        AVG(CASE WHEN ts.location = 'away' THEN ts.goals END)::numeric, 
        2
    ) AS rata_rata_gol_away,
    SUM(CASE WHEN ts.location = 'home' AND ts.result = 'win' THEN 3 
             WHEN ts.location = 'home' AND ts.result = 'draw' THEN 1 
             ELSE 0 END) - 
    SUM(CASE WHEN ts.location = 'away' AND ts.result = 'win' THEN 3 
             WHEN ts.location = 'away' AND ts.result = 'draw' THEN 1 
             ELSE 0 END) AS selisih_poin_home_vs_away
FROM teams t
INNER JOIN leagues l ON t.league_id = l.league_id
INNER JOIN team_stats ts ON t.team_id = ts.team_id
INNER JOIN games g ON ts.game_id = g.game_id
WHERE g.status = 'completed'
GROUP BY t.team_id, t.name, l.name
HAVING COUNT(CASE WHEN ts.location = 'home' THEN 1 END) >= 5
   AND COUNT(CASE WHEN ts.location = 'away' THEN 1 END) >= 5
ORDER BY selisih_poin_home_vs_away DESC;

-- Query 5.4: Analisis kontribusi pemain (gol + assist)
-- Penjelasan: Menghitung total kontribusi pemain dalam mencetak gol
SELECT 
    p.name AS nama_pemain,
    t.name AS tim_saat_ini,
    l.name AS liga,
    SUM(a.goals) AS total_gol,
    SUM(a.assists) AS total_assist,
    SUM(a.goals) + SUM(a.assists) AS total_kontribusi_gol,
    COUNT(DISTINCT a.game_id) AS jumlah_pertandingan,
    ROUND(
        ((SUM(a.goals) + SUM(a.assists))::numeric / 
         NULLIF(COUNT(DISTINCT a.game_id), 0)), 
        2
    ) AS kontribusi_per_pertandingan
FROM players p
INNER JOIN appearances a ON p.player_id = a.player_id
INNER JOIN games g ON a.game_id = g.game_id
INNER JOIN teams t ON a.team_id = t.team_id
INNER JOIN leagues l ON t.league_id = l.league_id
WHERE g.status = 'completed'
  AND p.is_active = true
GROUP BY p.player_id, p.name, t.team_id, t.name, l.name
HAVING (SUM(a.goals) + SUM(a.assists)) > 0
ORDER BY total_kontribusi_gol DESC
LIMIT 20;

-- ============================================================================
-- BAGIAN 6: QUERY DENGAN FUNGSI BUILT-IN
-- ============================================================================

-- Query 6.1: Analisis performa berdasarkan bulan
-- Penjelasan: Menggunakan EXTRACT() untuk mengambil bulan dari tanggal
-- Berguna untuk analisis musiman atau tren bulanan
SELECT 
    EXTRACT(YEAR FROM g.date) AS tahun,
    EXTRACT(MONTH FROM g.date) AS bulan,
    TO_CHAR(g.date, 'Month') AS nama_bulan,
    COUNT(DISTINCT g.game_id) AS jumlah_pertandingan,
    ROUND(AVG(g.home_goals + g.away_goals)::numeric, 2) AS rata_rata_gol_per_pertandingan
FROM games g
WHERE g.status = 'completed'
GROUP BY EXTRACT(YEAR FROM g.date), EXTRACT(MONTH FROM g.date), TO_CHAR(g.date, 'Month')
ORDER BY tahun DESC, bulan;

-- Query 6.2: Pertandingan dengan skor tertinggi
-- Penjelasan: Menggunakan fungsi agregasi MAX() dan string concatenation
SELECT 
    g.game_id,
    g.date AS tanggal,
    l.name AS liga,
    ht.name AS tim_tuan_rumah,
    at.name AS tim_tamu,
    g.home_goals || ' - ' || g.away_goals AS skor,
    g.home_goals + g.away_goals AS total_gol
FROM games g
INNER JOIN leagues l ON g.league_id = l.league_id
INNER JOIN teams ht ON g.home_team_id = ht.team_id
INNER JOIN teams at ON g.away_team_id = at.team_id
WHERE g.status = 'completed'
ORDER BY total_gol DESC, g.date DESC
LIMIT 10;

-- Query 6.3: Analisis kartu kuning dan merah
-- Penjelasan: Menggunakan COALESCE() untuk handle NULL values
SELECT 
    t.name AS nama_tim,
    l.name AS liga,
    SUM(ts.yellow_cards) AS total_kartu_kuning,
    SUM(ts.red_cards) AS total_kartu_merah,
    COUNT(DISTINCT ts.game_id) AS jumlah_pertandingan,
    ROUND(
        (SUM(ts.yellow_cards)::numeric / 
         NULLIF(COUNT(DISTINCT ts.game_id), 0)), 
        2
    ) AS rata_rata_kartu_kuning_per_pertandingan,
    ROUND(
        (SUM(ts.red_cards)::numeric / 
         NULLIF(COUNT(DISTINCT ts.game_id), 0)), 
        2
    ) AS rata_rata_kartu_merah_per_pertandingan
FROM teams t
INNER JOIN leagues l ON t.league_id = l.league_id
INNER JOIN team_stats ts ON t.team_id = ts.team_id
INNER JOIN games g ON ts.game_id = g.game_id
WHERE g.status = 'completed'
GROUP BY t.team_id, t.name, l.name
ORDER BY total_kartu_kuning DESC, total_kartu_merah DESC;

-- ============================================================================
-- BAGIAN 7: QUERY DENGAN VIEWS YANG SUDAH ADA
-- ============================================================================

-- Query 7.1: Menggunakan view v_games_full
-- Penjelasan: View sudah dibuat di DDL, kita bisa langsung query
-- View menyederhanakan query kompleks menjadi query sederhana
SELECT 
    league_name AS liga,
    season AS musim,
    COUNT(*) AS jumlah_pertandingan,
    COUNT(CASE WHEN winner != 'Draw' THEN 1 END) AS pertandingan_dengan_pemenang,
    COUNT(CASE WHEN winner = 'Draw' THEN 1 END) AS pertandingan_seri
FROM v_games_full
WHERE status = 'completed'
GROUP BY league_name, season
ORDER BY season DESC, league_name;

-- Query 7.2: Menggunakan view v_player_stats_summary
-- Penjelasan: View ini sudah melakukan agregasi, kita tinggal filter dan sort
SELECT 
    player_name AS nama_pemain,
    games_played AS jumlah_pertandingan,
    total_goals AS total_gol,
    total_assists AS total_assist,
    goals_per_90,
    assists_per_90
FROM v_player_stats_summary
WHERE total_goals > 0 OR total_assists > 0
ORDER BY (total_goals + total_assists) DESC
LIMIT 15;

-- Query 7.3: Menggunakan view v_team_performance
-- Penjelasan: View ini memberikan ringkasan performa tim
SELECT 
    team_name AS nama_tim,
    league_name AS liga,
    games_played AS jumlah_pertandingan,
    wins AS menang,
    draws AS seri,
    losses AS kalah,
    goals_scored AS gol_memasukkan,
    shot_accuracy_pct AS persentase_akurasi_tembakan
FROM v_team_performance
WHERE games_played >= 10
ORDER BY wins DESC, goals_scored DESC;

-- ============================================================================
-- BAGIAN 8: QUERY DENGAN MATERIALIZED VIEWS
-- ============================================================================

-- Query 8.1: Menggunakan materialized view mv_league_standings
-- Penjelasan: Materialized view sudah di-precompute, query lebih cepat
-- Perlu di-refresh secara berkala untuk data terbaru
SELECT 
    league_name AS liga,
    season AS musim,
    team_name AS nama_tim,
    matches_played AS jumlah_pertandingan,
    points AS poin,
    wins AS menang,
    draws AS seri,
    losses AS kalah,
    goals_for AS gol_memasukkan,
    goals_against AS gol_kemasukan,
    goal_difference AS selisih_gol
FROM mv_league_standings
WHERE season = (SELECT MAX(season) FROM mv_league_standings)
ORDER BY league_name, points DESC, goal_difference DESC;

-- Query 8.2: Top scorer menggunakan materialized view
-- Penjelasan: Materialized view sudah menghitung agregasi, query lebih efisien
SELECT 
    league_name AS liga,
    season AS musim,
    player_name AS nama_pemain,
    games_played AS jumlah_pertandingan,
    total_goals AS total_gol,
    total_assists AS total_assist,
    goals_per_90
FROM mv_top_scorers
WHERE season = (SELECT MAX(season) FROM mv_top_scorers)
ORDER BY league_name, total_goals DESC;

-- ============================================================================
-- BAGIAN 9: QUERY DENGAN FUNCTIONS YANG SUDAH ADA
-- ============================================================================

-- Query 9.1: Menggunakan function get_team_form()
-- Penjelasan: Function ini sudah dibuat di DDL untuk mendapatkan form tim
-- Mengganti parameter p_team_id dengan ID tim yang diinginkan
-- Contoh: Mendapatkan form 5 pertandingan terakhir untuk tim dengan ID 1
SELECT * FROM get_team_form(1, 5);

-- Query 9.2: Menggunakan function get_head_to_head()
-- Penjelasan: Function untuk melihat head-to-head antara dua tim
-- Parameter: team1_id, team2_id, dan optional season
-- Contoh: Head-to-head antara tim ID 1 dan ID 2
SELECT * FROM get_head_to_head(1, 2);

-- Query 9.3: Head-to-head untuk musim tertentu
-- Penjelasan: Menambahkan parameter season untuk filter musim
SELECT * FROM get_head_to_head(1, 2, 2023);

-- ============================================================================
-- BAGIAN 10: QUERY ANALITIK LANJUTAN
-- ============================================================================

-- Query 10.1: Analisis shot quality berdasarkan xG
-- Penjelasan: Menganalisis kualitas tembakan berdasarkan expected goals
SELECT 
    s.shot_result AS hasil_tembakan,
    s.situation AS situasi,
    COUNT(*) AS jumlah_tembakan,
    ROUND(AVG(s.x_goal)::numeric, 3) AS rata_rata_xg,
    SUM(CASE WHEN s.shot_result = 'Goal' THEN 1 ELSE 0 END) AS jumlah_gol,
    ROUND(
        (SUM(CASE WHEN s.shot_result = 'Goal' THEN 1 ELSE 0 END)::numeric / 
         NULLIF(COUNT(*), 0) * 100), 
        2
    ) AS persentase_konversi
FROM shots s
INNER JOIN games g ON s.game_id = g.game_id
WHERE g.status = 'completed'
  AND s.x_goal IS NOT NULL
GROUP BY s.shot_result, s.situation
ORDER BY rata_rata_xg DESC;

-- Query 10.2: Analisis assist dan key passes
-- Penjelasan: Menganalisis kontribusi pemain dalam menciptakan peluang
SELECT 
    p.name AS nama_pemain,
    t.name AS tim,
    SUM(a.assists) AS total_assist,
    SUM(a.key_passes) AS total_key_passes,
    ROUND(AVG(a.x_assists)::numeric, 3) AS rata_rata_x_assist,
    ROUND(
        (SUM(a.assists)::numeric / 
         NULLIF(SUM(a.key_passes), 0)), 
        2
    ) AS rasio_assist_per_key_pass
FROM players p
INNER JOIN appearances a ON p.player_id = a.player_id
INNER JOIN games g ON a.game_id = g.game_id
INNER JOIN teams t ON a.team_id = t.team_id
WHERE g.status = 'completed'
  AND p.is_active = true
GROUP BY p.player_id, p.name, t.team_id, t.name
HAVING SUM(a.key_passes) > 0
ORDER BY total_assist DESC, total_key_passes DESC
LIMIT 20;

-- Query 10.3: Analisis performa berdasarkan posisi pemain
-- Penjelasan: Menganalisis statistik berdasarkan posisi pemain di lapangan
SELECT 
    a.position AS posisi,
    COUNT(DISTINCT a.player_id) AS jumlah_pemain,
    COUNT(DISTINCT a.game_id) AS jumlah_penampilan,
    ROUND(AVG(a.goals)::numeric, 3) AS rata_rata_gol_per_penampilan,
    ROUND(AVG(a.assists)::numeric, 3) AS rata_rata_assist_per_penampilan,
    ROUND(AVG(a.time_played)::numeric, 1) AS rata_rata_menit_bermain,
    ROUND(AVG(a.x_goals)::numeric, 3) AS rata_rata_xg
FROM appearances a
INNER JOIN games g ON a.game_id = g.game_id
WHERE g.status = 'completed'
  AND a.position IS NOT NULL
GROUP BY a.position
ORDER BY rata_rata_gol_per_penampilan DESC;

-- Query 10.4: Analisis performa tim berdasarkan waktu (half-time vs full-time)
-- Penjelasan: Membandingkan performa di babak pertama vs akhir pertandingan
SELECT 
    t.name AS nama_tim,
    l.name AS liga,
    COUNT(DISTINCT g.game_id) AS jumlah_pertandingan,
    SUM(CASE 
        WHEN ts.location = 'home' AND g.home_goals_half_time > g.away_goals_half_time THEN 1
        WHEN ts.location = 'away' AND g.away_goals_half_time > g.home_goals_half_time THEN 1
        ELSE 0
    END) AS menang_di_babak_pertama,
    SUM(CASE 
        WHEN ts.location = 'home' AND g.home_goals > g.away_goals THEN 1
        WHEN ts.location = 'away' AND g.away_goals > g.home_goals THEN 1
        ELSE 0
    END) AS menang_di_akhir_pertandingan,
    SUM(CASE 
        WHEN (ts.location = 'home' AND g.home_goals_half_time <= g.away_goals_half_time 
              AND g.home_goals > g.away_goals) OR
             (ts.location = 'away' AND g.away_goals_half_time <= g.home_goals_half_time 
              AND g.away_goals > g.home_goals)
        THEN 1 ELSE 0
    END) AS comeback_wins
FROM teams t
INNER JOIN leagues l ON t.league_id = l.league_id
INNER JOIN team_stats ts ON t.team_id = ts.team_id
INNER JOIN games g ON ts.game_id = g.game_id
WHERE g.status = 'completed'
  AND g.home_goals_half_time IS NOT NULL
  AND g.away_goals_half_time IS NOT NULL
GROUP BY t.team_id, t.name, l.name
HAVING COUNT(DISTINCT g.game_id) >= 10
ORDER BY comeback_wins DESC, menang_di_akhir_pertandingan DESC;

-- ============================================================================
-- BAGIAN 11: QUERY UNTUK VALIDASI DATA
-- ============================================================================

-- Query 11.1: Validasi konsistensi hasil pertandingan
-- Penjelasan: Menggunakan function yang sudah dibuat untuk validasi
SELECT * FROM validate_game_results();

-- Query 11.2: Mencari pertandingan dengan data tidak lengkap
-- Penjelasan: Mencari pertandingan yang tidak memiliki team_stats lengkap
SELECT 
    g.game_id,
    g.date AS tanggal,
    ht.name AS tim_tuan_rumah,
    at.name AS tim_tamu,
    COUNT(ts.team_stat_id) AS jumlah_team_stats,
    CASE 
        WHEN COUNT(ts.team_stat_id) = 0 THEN 'Tidak ada statistik'
        WHEN COUNT(ts.team_stat_id) = 1 THEN 'Hanya satu tim'
        WHEN COUNT(ts.team_stat_id) = 2 THEN 'Lengkap'
        ELSE 'Lebih dari dua (error)'
    END AS status_data
FROM games g
INNER JOIN teams ht ON g.home_team_id = ht.team_id
INNER JOIN teams at ON g.away_team_id = at.team_id
LEFT JOIN team_stats ts ON g.game_id = ts.game_id
WHERE g.status = 'completed'
GROUP BY g.game_id, g.date, ht.name, at.name
HAVING COUNT(ts.team_stat_id) != 2
ORDER BY g.date DESC;

-- Query 11.3: Mencari pemain yang muncul untuk tim yang tidak bermain
-- Penjelasan: Validasi data integrity untuk appearances
SELECT 
    a.appearance_id,
    p.name AS nama_pemain,
    t.name AS tim_pemain,
    g.game_id,
    ht.name AS tim_tuan_rumah,
    at.name AS tim_tamu
FROM appearances a
INNER JOIN players p ON a.player_id = p.player_id
INNER JOIN teams t ON a.team_id = t.team_id
INNER JOIN games g ON a.game_id = g.game_id
INNER JOIN teams ht ON g.home_team_id = ht.team_id
INNER JOIN teams at ON g.away_team_id = at.team_id
WHERE g.status = 'completed'
  AND a.team_id != g.home_team_id 
  AND a.team_id != g.away_team_id;

-- ============================================================================
-- BAGIAN 12: QUERY OPTIMASI DAN BEST PRACTICES
-- ============================================================================

-- Query 12.1: Query dengan index optimization
-- Penjelasan: Query ini memanfaatkan index yang sudah dibuat di DDL
-- Menggunakan WHERE clause pada kolom yang ter-index
SELECT 
    p.name AS nama_pemain,
    SUM(a.goals) AS total_gol
FROM players p
INNER JOIN appearances a ON p.player_id = a.player_id
INNER JOIN games g ON a.game_id = g.game_id
WHERE p.is_active = true  -- Memanfaatkan index pada is_active
  AND g.status = 'completed'  -- Memanfaatkan index pada status
  AND g.season = 2023  -- Memanfaatkan index pada season
GROUP BY p.player_id, p.name
HAVING SUM(a.goals) > 0
ORDER BY total_gol DESC;

-- Query 12.2: Query dengan EXPLAIN untuk analisis performa
-- Penjelasan: EXPLAIN menunjukkan execution plan query
-- Berguna untuk optimasi query (uncomment untuk melihat plan)
-- EXPLAIN ANALYZE
SELECT 
    t.name AS nama_tim,
    COUNT(DISTINCT ts.game_id) AS jumlah_pertandingan
FROM teams t
INNER JOIN team_stats ts ON t.team_id = ts.team_id
INNER JOIN games g ON ts.game_id = g.game_id
WHERE g.status = 'completed'
GROUP BY t.team_id, t.name;

-- Query 12.3: Query dengan pagination untuk performa
-- Penjelasan: Menggunakan LIMIT dan OFFSET untuk pagination
-- Lebih efisien daripada mengambil semua data sekaligus
SELECT 
    p.name AS nama_pemain,
    SUM(a.goals) AS total_gol,
    SUM(a.assists) AS total_assist
FROM players p
INNER JOIN appearances a ON p.player_id = a.player_id
INNER JOIN games g ON a.game_id = g.game_id
WHERE g.status = 'completed'
GROUP BY p.player_id, p.name
ORDER BY (SUM(a.goals) + SUM(a.assists)) DESC
LIMIT 20 OFFSET 0;  -- Halaman pertama (0-19)
-- Untuk halaman berikutnya, ganti OFFSET: OFFSET 20, OFFSET 40, dst.

-- ============================================================================
-- BAGIAN 13: QUERY UNTUK LAPORAN KOMPREHENSIF
-- ============================================================================

-- Query 13.1: Laporan lengkap performa liga
-- Penjelasan: Laporan komprehensif untuk setiap liga
SELECT 
    l.name AS liga,
    l.country AS negara,
    COUNT(DISTINCT g.season) AS jumlah_musim,
    COUNT(DISTINCT g.game_id) AS total_pertandingan,
    COUNT(DISTINCT t.team_id) AS jumlah_tim,
    ROUND(AVG(g.home_goals + g.away_goals)::numeric, 2) AS rata_rata_gol_per_pertandingan,
    SUM(CASE WHEN g.home_goals = g.away_goals THEN 1 ELSE 0 END) AS jumlah_seri,
    ROUND(
        (SUM(CASE WHEN g.home_goals = g.away_goals THEN 1 ELSE 0 END)::numeric / 
         NULLIF(COUNT(g.game_id), 0) * 100), 
        2
    ) AS persentase_seri
FROM leagues l
LEFT JOIN games g ON l.league_id = g.league_id AND g.status = 'completed'
LEFT JOIN teams t ON l.league_id = t.league_id AND t.is_active = true
WHERE l.is_active = true
GROUP BY l.league_id, l.name, l.country
ORDER BY total_pertandingan DESC;

-- Query 13.2: Laporan performa pemain per musim
-- Penjelasan: Laporan detail performa pemain untuk setiap musim
SELECT 
    p.name AS nama_pemain,
    g.season AS musim,
    l.name AS liga,
    COUNT(DISTINCT a.game_id) AS jumlah_pertandingan,
    SUM(a.goals) AS total_gol,
    SUM(a.assists) AS total_assist,
    SUM(a.time_played) AS total_menit,
    ROUND((SUM(a.goals)::numeric / NULLIF(SUM(a.time_played), 0) * 90), 2) AS gol_per_90,
    ROUND((SUM(a.assists)::numeric / NULLIF(SUM(a.time_played), 0) * 90), 2) AS assist_per_90,
    ROUND(AVG(a.x_goals)::numeric, 3) AS rata_rata_xg
FROM players p
INNER JOIN appearances a ON p.player_id = a.player_id
INNER JOIN games g ON a.game_id = g.game_id
INNER JOIN leagues l ON g.league_id = l.league_id
WHERE g.status = 'completed'
  AND p.is_active = true
GROUP BY p.player_id, p.name, g.season, l.league_id, l.name
HAVING SUM(a.goals) > 0 OR SUM(a.assists) > 0
ORDER BY g.season DESC, (SUM(a.goals) + SUM(a.assists)) DESC;

-- Query 13.3: Laporan head-to-head antar liga
-- Penjelasan: Analisis perbandingan statistik antar liga
SELECT 
    l1.name AS liga_1,
    l2.name AS liga_2,
    COUNT(DISTINCT g1.game_id) AS pertandingan_liga_1,
    COUNT(DISTINCT g2.game_id) AS pertandingan_liga_2,
    ROUND(AVG(g1.home_goals + g1.away_goals)::numeric, 2) AS rata_rata_gol_liga_1,
    ROUND(AVG(g2.home_goals + g2.away_goals)::numeric, 2) AS rata_rata_gol_liga_2
FROM leagues l1
CROSS JOIN leagues l2
LEFT JOIN games g1 ON l1.league_id = g1.league_id AND g1.status = 'completed'
LEFT JOIN games g2 ON l2.league_id = g2.league_id AND g2.status = 'completed'
WHERE l1.league_id < l2.league_id  -- Menghindari duplikasi
  AND l1.is_active = true
  AND l2.is_active = true
GROUP BY l1.league_id, l1.name, l2.league_id, l2.name
HAVING COUNT(DISTINCT g1.game_id) > 0 AND COUNT(DISTINCT g2.game_id) > 0
ORDER BY l1.name, l2.name;

-- ============================================================================
-- CATATAN PENTING UNTUK LIVE CODING
-- ============================================================================

/*
TIPS UNTUK PRESENTASI LIVE CODING:

1. MULAI DARI YANG SEDERHANA
   - Mulai dengan query SELECT dasar
   - Jelaskan setiap bagian (SELECT, FROM, WHERE, dll)
   - Tambahkan kompleksitas secara bertahap

2. JELASKAN KONSEP PENTING
   - JOIN: INNER, LEFT, RIGHT, FULL
   - Agregasi: COUNT, SUM, AVG, MAX, MIN
   - GROUP BY dan HAVING
   - Subquery vs CTE
   - Window Functions

3. DEMONSTRASIKAN BEST PRACTICES
   - Gunakan alias untuk readability
   - Format query dengan baik
   - Gunakan index yang tepat
   - Validasi data integrity

4. SIAPKAN CONTOH DATA
   - Pastikan database sudah terisi data
   - Test query sebelum presentasi
   - Siapkan backup plan jika query error

5. JELASKAN HASIL
   - Interpretasikan hasil query
   - Hubungkan dengan business logic
   - Diskusikan optimasi jika perlu

6. HANDLE ERROR DENGAN BAIK
   - Jangan panik jika ada error
   - Jelaskan penyebab error
   - Perbaiki dengan tenang
   - Gunakan sebagai teaching moment

7. INTERAKSI DENGAN AUDIENCE
   - Tanyakan apakah ada pertanyaan
   - Jelaskan konsep yang mungkin membingungkan
   - Berikan contoh use case yang relevan
*/

-- ============================================================================
-- END OF DQL FILE
-- ============================================================================
