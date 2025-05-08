const { Pool, types } = require('pg');
const config = require('./config.json');

types.setTypeParser(20, val => parseInt(val, 10));

const connection = new Pool({
  host: config.rds_host,
  user: config.rds_user,
  password: config.rds_password,
  port: config.rds_port,
  database: config.rds_db,
  ssl: { rejectUnauthorized: false },
});

connection.connect((err) => err && console.error(err));

const home = async (req, res) => {
  res.json({
    authors: ['Arriella Mafuta', 'Xiang Chen', 'Lucas Lee', 'Tiffany Lian'],
    description: 'Song recommendation app using playlists, albums, and track attributes.'
  });
};

const top_songs = async (req, res) => {
  const limit = parseInt(req.query.limit) || 20;
  const year = req.query.year;
  const values = [];

  let query = `
    SELECT t.track_id, 
           t.name AS track_name, 
           STRING_AGG(DISTINCT a.name, ', ') AS artists, 
           COUNT(DISTINCT pt.playlist_id) AS playlist_count
    FROM tracks_import t
    JOIN artists a ON t.artist_id = a.artist_id
    JOIN playlists_import pt ON t.track_id = pt.track_id
  `;
  if (year) {
    query += ` WHERE t.year = $1`;
    values.push(year);
  }
  query += `
    GROUP BY t.track_id, t.name
    ORDER BY playlist_count DESC
    LIMIT $${values.length + 1}
  `;
  values.push(limit);

  connection.query(query, values, (err, data) => {
    if (err) res.status(500).json({ error: 'Database query failed' });
    else res.json(data.rows);
  });
};

const top_albums = async (req, res) => {
  const year = req.query.year;
  const limit = parseInt(req.query.limit) || 20;
  const values = [];

  let query = `
    SELECT al.album_id, al.name AS album_name, ar.name AS artist_name, COUNT(pt.playlist_id) AS playlist_count
    FROM albums al
    JOIN artists ar ON al.artist_id = ar.artist_id
    JOIN tracks_import t ON al.album_id = t.album_id
    JOIN playlists_import pt ON t.track_id = pt.track_id
  `;
  if (year) {
    query += ` WHERE t.year = $1`;
    values.push(year);
  }
  query += `
    GROUP BY al.album_id, al.name, ar.name
    ORDER BY playlist_count DESC
    LIMIT $${values.length + 1}
  `;
  values.push(limit);

  connection.query(query, values, (err, data) => {
    if (err) res.status(500).json({ error: 'Database query failed' });
    else res.json(data.rows);
  });
};

const top_playlists = async (req, res) => {
  const limit = parseInt(req.query.limit) || 20;

  connection.query(`
    SELECT playlist_id, name, followers
    FROM playlists
    ORDER BY followers DESC
    LIMIT $1
  `, [limit], (err, data) => {
    if (err) res.status(500).json({ error: 'Database query failed' });
    else res.json(data.rows);
  });
};

const search_songs = async (req, res) => {
  const { name = '', artist = '', year, limit = 20 } = req.query;
  const conditions = [];
  const values = [];

  if (name) {
    conditions.push(`t.name ILIKE $${values.length + 1}`);
    values.push(`%${name}%`);
  }
  if (artist) {
    conditions.push(`a.name ILIKE $${values.length + 1}`);
    values.push(`%${artist}%`);
  }
  if (year) {
    conditions.push(`t.year = $${values.length + 1}`);
    values.push(year);
  }

  const whereClause = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
  const query = `
    SELECT t.track_id, 
           t.name AS track_name, 
           STRING_AGG(DISTINCT a.name, ', ') AS artist_names, 
           COUNT(DISTINCT pt.playlist_id) AS playlist_count
    FROM tracks_import t
    JOIN artists a ON t.artist_id = a.artist_id
    JOIN playlists_import pt ON t.track_id = pt.track_id
    ${whereClause}
    GROUP BY t.track_id, t.name
    ORDER BY playlist_count DESC
    LIMIT $${values.length + 1}
  `;
  values.push(limit);

  connection.query(query, values, (err, data) => {
    if (err) res.status(500).json({ error: 'Database query failed' });
    else res.json(data.rows);
  });
};

const search_albums = async (req, res) => {
  const { name = '', artist = '', limit = 20 } = req.query;
  const conditions = [];
  const values = [];

  if (name) {
    conditions.push(`al.name ILIKE $${values.length + 1}`);
    values.push(`%${name}%`);
  }
  if (artist) {
    conditions.push(`ar.name ILIKE $${values.length + 1}`);
    values.push(`%${artist}%`);
  }

  const whereClause = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
  const query = `
    SELECT al.album_id, al.name AS album_name, ar.name AS artist_name, COUNT(DISTINCT pt.playlist_id) AS playlist_count
    FROM albums al
    JOIN artists ar ON al.artist_id = ar.artist_id
    JOIN tracks_import t ON al.album_id = t.album_id
    JOIN playlists_import pt ON t.track_id = pt.track_id
    ${whereClause}
    GROUP BY al.album_id, al.name, ar.name
    ORDER BY playlist_count DESC
    LIMIT $${values.length + 1}
  `;
  values.push(limit);

  connection.query(query, values, (err, data) => {
    if (err) res.status(500).json({ error: 'Database query failed' });
    else res.json(data.rows);
  });
};

const search_playlists = async (req, res) => {
  const { name = '', limit = 20 } = req.query;
  const nameFilter = `%${name}%`;

  const query = `
    SELECT p.playlist_id, p.name, COUNT(pt.track_id) AS song_count, p.followers
    FROM playlists p
    JOIN playlists_import pt ON p.playlist_id = pt.playlist_id
    WHERE p.name ILIKE $1
    GROUP BY p.playlist_id, p.name, p.followers
    ORDER BY followers DESC
    LIMIT $2
  `;

  connection.query(query, [nameFilter, limit], (err, data) => {
    if (err) res.status(500).json({ error: 'Database query failed' });
    else res.json(data.rows);
  });
};

const recommend_song_on_song = async (req, res) => {
  const { track_id, limit = 10 } = req.query;

  const query = `
    SELECT t2.track_id, 
           t2.name,
           STRING_AGG(DISTINCT a.name, ', ') AS artist_names,
      (t1.danceability * t2.danceability +
       t1.energy * t2.energy +
       t1.liveness * t2.liveness +
       t1.key * t2.key +
       t1.loudness * t2.loudness +
       t1.speechiness * t2.speechiness +
       t1.acousticness * t2.acousticness +
       t1.valence * t2.valence +
       t1.tempo * t2.tempo) /
      (NULLIF(SQRT(POWER(t1.danceability,2) + POWER(t1.energy,2) + POWER(t1.liveness,2) +
                  POWER(t1.key,2) + POWER(t1.loudness,2) + POWER(t1.speechiness,2) +
                  POWER(t1.acousticness,2) + POWER(t1.valence,2) + POWER(t1.tempo,2)), 0) *
       NULLIF(SQRT(POWER(t2.danceability,2) + POWER(t2.energy,2) + POWER(t2.liveness,2) +
                  POWER(t2.key,2) + POWER(t2.loudness,2) + POWER(t2.speechiness,2) +
                  POWER(t2.acousticness,2) + POWER(t2.valence,2) + POWER(t2.tempo,2)), 0)) AS similarity
    FROM tracks_import t1
    JOIN tracks_import t2 ON t1.track_id <> t2.track_id
    JOIN artists a ON t2.artist_id = a.artist_id
    WHERE t1.track_id = $1
    GROUP BY t2.track_id, t2.name, similarity
    ORDER BY similarity DESC
    LIMIT $2;
  `;

  connection.query(query, [track_id, limit], (err, data) => {
    if (err) res.status(500).json({ error: 'Database query failed' });
    else res.json(data.rows);
  });
};

const recommend_song_on_artist = async (req, res) => {
  const { artist_id, limit = 10 } = req.query;

  const query = `
    WITH artist_avg AS (
      SELECT AVG(danceability) AS danceability,
             AVG(energy) AS energy,
             AVG(liveness) AS liveness,
             AVG(key) AS key,
             AVG(loudness) AS loudness,
             AVG(speechiness) AS speechiness,
             AVG(acousticness) AS acousticness,
             AVG(valence) AS valence,
             AVG(tempo) AS tempo
      FROM tracks_import
      WHERE artist_id = $1
    )
    SELECT t.track_id, 
           t.name, 
           STRING_AGG(DISTINCT a.name, ', ') AS artist_names,
      (t.danceability * artist_avg.danceability +
       t.energy * artist_avg.energy +
       t.liveness * artist_avg.liveness +
       t.key * artist_avg.key +
       t.loudness * artist_avg.loudness +
       t.speechiness * artist_avg.speechiness +
       t.acousticness * artist_avg.acousticness +
       t.valence * artist_avg.valence +
       t.tempo * artist_avg.tempo) /
      (NULLIF(SQRT(POWER(t.danceability,2) + POWER(t.energy,2) + POWER(t.liveness,2) +
                  POWER(t.key,2) + POWER(t.loudness,2) + POWER(t.speechiness,2) +
                  POWER(t.acousticness,2) + POWER(t.valence,2) + POWER(t.tempo,2)), 0) *
       NULLIF(SQRT(POWER(artist_avg.danceability,2) + POWER(artist_avg.energy,2) + POWER(artist_avg.liveness,2) +
                  POWER(artist_avg.key,2) + POWER(artist_avg.loudness,2) + POWER(artist_avg.speechiness,2) +
                  POWER(artist_avg.acousticness,2) + POWER(artist_avg.valence,2) + POWER(artist_avg.tempo,2)), 0)) AS similarity
    FROM tracks_import t
    JOIN artists a ON t.artist_id = a.artist_id, artist_avg
    WHERE t.artist_id <> $1
    GROUP BY t.track_id, t.name, similarity
    ORDER BY similarity DESC
    LIMIT $2;
  `;

  const values = [artist_id, limit];

  try {
    const result = await connection.query(query, values);
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Database query failed' });
  }
};

const recommend_song_on_playlist = async (req, res) => {
  const { playlist_id, limit = 10 } = req.query;

  const query = `
    WITH playlist_avg AS (
      SELECT AVG(t.danceability) AS danceability,
             AVG(t.energy) AS energy,
             AVG(t.liveness) AS liveness,
             AVG(t.key) AS key,
             AVG(t.loudness) AS loudness,
             AVG(t.speechiness) AS speechiness,
             AVG(t.acousticness) AS acousticness,
             AVG(t.valence) AS valence,
             AVG(t.tempo) AS tempo
      FROM playlists_import pt
      JOIN tracks_import t ON pt.track_id = t.track_id
      WHERE pt.playlist_id = $1
    )
    SELECT t.track_id, 
           t.name, 
           STRING_AGG(DISTINCT a.name, ', ') AS artist_names,
      (t.danceability * playlist_avg.danceability +
       t.energy * playlist_avg.energy +
       t.liveness * playlist_avg.liveness +
       t.key * playlist_avg.key +
       t.loudness * playlist_avg.loudness +
       t.speechiness * playlist_avg.speechiness +
       t.acousticness * playlist_avg.acousticness +
       t.valence * playlist_avg.valence +
       t.tempo * playlist_avg.tempo) /
      (NULLIF(SQRT(POWER(t.danceability,2) + POWER(t.energy,2) + POWER(t.liveness,2) +
                  POWER(t.key,2) + POWER(t.loudness,2) + POWER(t.speechiness,2) +
                  POWER(t.acousticness,2) + POWER(t.valence,2) + POWER(t.tempo,2)), 0) *
       NULLIF(SQRT(POWER(playlist_avg.danceability,2) + POWER(playlist_avg.energy,2) + POWER(playlist_avg.liveness,2) +
                  POWER(playlist_avg.key,2) + POWER(playlist_avg.loudness,2) + POWER(playlist_avg.speechiness,2) +
                  POWER(playlist_avg.acousticness,2) + POWER(playlist_avg.valence,2) + POWER(playlist_avg.tempo,2)), 0)) AS similarity
    FROM tracks_import t
    JOIN artists a ON t.artist_id = a.artist_id, playlist_avg
    WHERE t.track_id NOT IN (
      SELECT track_id FROM playlists_import WHERE playlist_id = $1
    )
    GROUP BY t.track_id, t.name, similarity
    ORDER BY similarity DESC
    LIMIT $2;
  `;

  const values = [playlist_id, limit];

  try {
    const result = await connection.query(query, values);
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Database query failed' });
  }
};

const recommend_playlist_on_song = async (req, res) => {
  const { track_id, limit = 10 } = req.query;

  const query = `
    WITH song_attributes AS (
      SELECT danceability, energy, liveness, key, loudness,
             speechiness, acousticness, valence, tempo
      FROM tracks_import
      WHERE id = $1
    ),
    playlist_attributes AS (
      SELECT p.playlist_id, p.name,
             AVG(t.danceability) AS danceability,
             AVG(t.energy) AS energy,
             AVG(t.liveness) AS liveness,
             AVG(t.key) AS key,
             AVG(t.loudness) AS loudness,
             AVG(t.speechiness) AS speechiness,
             AVG(t.acousticness) AS acousticness,
             AVG(t.valence) AS valence,
             AVG(t.tempo) AS tempo
      FROM playlists p
      JOIN playlists_import pt ON p.playlist_id = pt.playlist_id
      JOIN tracks_import t ON pt.track_id = t.track_id
      WHERE NOT EXISTS (
        SELECT 1
        FROM playlists_import pt_check
        WHERE pt_check.playlist_id = p.playlist_id
          AND pt_check.track_id = $1
      )
      GROUP BY p.playlist_id, p.name
    )
    SELECT pa.playlist_id, pa.name,
      (pa.danceability * sa.danceability +
       pa.energy * sa.energy +
       pa.liveness * sa.liveness +
       pa.key * sa.key +
       pa.loudness * sa.loudness +
       pa.speechiness * sa.speechiness +
       pa.acousticness * sa.acousticness +
       pa.valence * sa.valence +
       pa.tempo * sa.tempo) /
      (NULLIF(SQRT(POWER(pa.danceability,2) + POWER(pa.energy,2) + POWER(pa.liveness,2) +
                  POWER(pa.key,2) + POWER(pa.loudness,2) + POWER(pa.speechiness,2) +
                  POWER(pa.acousticness,2) + POWER(pa.valence,2) + POWER(pa.tempo,2)), 0) *
       NULLIF(SQRT(POWER(sa.danceability,2) + POWER(sa.energy,2) + POWER(sa.liveness,2) +
                  POWER(sa.key,2) + POWER(sa.loudness,2) + POWER(sa.speechiness,2) +
                  POWER(sa.acousticness,2) + POWER(sa.valence,2) + POWER(sa.tempo,2)), 0)) AS similarity
    FROM playlist_attributes pa, song_attributes sa
    ORDER BY similarity DESC
    LIMIT $2;
  `;

  try {
    const result = await connection.query(query, [track_id, limit]);
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Database query failed' });
  }
};

const recommend_artists_by_similarity = async (req, res) => {
  const { artist_id, limit = 10 } = req.query;

  const query = `
    WITH target_avg AS (
      SELECT AVG(danceability) AS danceability,
             AVG(energy) AS energy,
             AVG(liveness) AS liveness,
             AVG(key) AS key,
             AVG(loudness) AS loudness,
             AVG(speechiness) AS speechiness,
             AVG(acousticness) AS acousticness,
             AVG(valence) AS valence,
             AVG(tempo) AS tempo
      FROM tracks_import
      WHERE artist_id = $1
    ),
    artist_avg AS (
      SELECT a.artist_id, a.name,
             AVG(t.danceability) AS danceability,
             AVG(t.energy) AS energy,
             AVG(t.liveness) AS liveness,
             AVG(t.key) AS key,
             AVG(t.loudness) AS loudness,
             AVG(t.speechiness) AS speechiness,
             AVG(t.acousticness) AS acousticness,
             AVG(t.valence) AS valence,
             AVG(t.tempo) AS tempo
      FROM artists a
      JOIN tracks_import t ON a.artist_id = t.artist_id
      WHERE a.artist_id <> $1
      GROUP BY a.artist_id, a.name
    )
    SELECT artist_avg.artist_id, artist_avg.name,
      (artist_avg.danceability * target_avg.danceability +
       artist_avg.energy * target_avg.energy +
       artist_avg.liveness * target_avg.liveness +
       artist_avg.key * target_avg.key +
       artist_avg.loudness * target_avg.loudness +
       artist_avg.speechiness * target_avg.speechiness +
       artist_avg.acousticness * target_avg.acousticness +
       artist_avg.valence * target_avg.valence +
       artist_avg.tempo * target_avg.tempo) /
      (SQRT(POWER(artist_avg.danceability,2) + POWER(artist_avg.energy,2) + POWER(artist_avg.liveness,2) +
            POWER(artist_avg.key,2) + POWER(artist_avg.loudness,2) + POWER(artist_avg.speechiness,2) +
            POWER(artist_avg.acousticness,2) + POWER(artist_avg.valence,2) + POWER(artist_avg.tempo,2)) *
       SQRT(POWER(target_avg.danceability,2) + POWER(target_avg.energy,2) + POWER(target_avg.liveness,2) +
            POWER(target_avg.key,2) + POWER(target_avg.loudness,2) + POWER(target_avg.speechiness,2) +
            POWER(target_avg.acousticness,2) + POWER(target_avg.valence,2) + POWER(target_avg.tempo,2))) AS similarity
    FROM artist_avg, target_avg
    ORDER BY similarity DESC
    LIMIT $2;
  `;

  try {
    const result = await connection.query(query, [artist_id, limit]);
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Database query failed' });
  }
};

const artist_stats = async (req, res) => {
  const { artist_id } = req.query;
  if (!artist_id) return res.status(400).json({ error: 'artist_id is required' });

  const query = `
    WITH artist_info AS (
      SELECT a.artist_id, a.name
      FROM artists a
      WHERE a.artist_id = $1
    ),
    top_song AS (
      SELECT t.track_id, t.name, COUNT(pt.playlist_id) AS playlist_count
      FROM tracks_import t
      JOIN playlists_import pt ON t.track_id = pt.track_id
      WHERE t.artist_id = $1
      GROUP BY t.track_id, t.name
      ORDER BY playlist_count DESC
      LIMIT 1
    ),
    stats_by_year AS (
      SELECT t.year,
             AVG(t.danceability) AS avg_danceability,
             AVG(t.energy) AS avg_energy,
             AVG(t.valence) AS avg_valence,
             AVG(t.acousticness) AS avg_acousticness,
             AVG(t.loudness) AS avg_loudness,
             COUNT(pt.playlist_id) AS playlist_count
      FROM tracks_import t
      LEFT JOIN playlists_import pt ON t.track_id = pt.track_id
      WHERE t.artist_id = $1
      GROUP BY t.year
      ORDER BY t.year
    ),
    overall_stats AS (
      SELECT AVG(t.danceability) AS danceability,
             AVG(t.energy) AS energy,
             AVG(t.valence) AS valence,
             AVG(t.acousticness) AS acousticness,
             AVG(t.loudness) AS loudness
      FROM tracks_import t
      WHERE t.artist_id = $1
    )
    SELECT ai.artist_id, ai.name AS artist_name,
           (SELECT json_build_object('track_id', ts.track_id, 'name', ts.name, 'playlist_count', ts.playlist_count)
            FROM top_song ts),
           (SELECT json_agg(sy) FROM stats_by_year sy),
           (SELECT CASE
               WHEN os.valence > 0.7 AND os.energy > 0.6 THEN 'happy'
               WHEN os.valence < 0.3 AND os.energy < 0.5 THEN 'sad'
               WHEN os.acousticness > 0.7 AND os.energy < 0.4 THEN 'chill'
               WHEN os.energy > 0.7 AND os.danceability > 0.6 THEN 'hype'
               ELSE 'unknown'
           END FROM overall_stats os) AS inferred_mood
    FROM artist_info ai;
  `;

  try {
    const result = await connection.query(query, [artist_id]);
    if (result.rowCount === 0) {
      return res.status(404).json({ error: 'Artist not found' });
    }
    res.json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Database query failed' });
  }
};

const recommend_playlists_by_mood = async (req, res) => {
  const { mood, limit = 10 } = req.query;

  const moodConditions = {
    happy: 'pf.avg_valence > 0.7 AND pf.avg_energy > 0.6',
    sad: 'pf.avg_valence < 0.3 AND pf.avg_energy < 0.5',
    chill: 'pf.avg_acousticness > 0.7 AND pf.avg_energy < 0.4',
    hype: 'pf.avg_energy > 0.7 AND pf.avg_danceability > 0.6',
  };

  if (!moodConditions[mood]) {
    return res.status(400).json({
      error: 'Invalid mood. Available moods: happy, sad, chill, hype',
    });
  }

  const query = `
    WITH playlist_features AS (
      SELECT p.playlist_id, p.name, p.followers,
             AVG(t.danceability) AS avg_danceability,
             AVG(t.energy) AS avg_energy,
             AVG(t.liveness) AS avg_liveness,
             AVG(t.key) AS avg_key,
             AVG(t.loudness) AS avg_loudness,
             AVG(t.speechiness) AS avg_speechiness,
             AVG(t.acousticness) AS avg_acousticness,
             AVG(t.valence) AS avg_valence,
             AVG(t.tempo) AS avg_tempo,
             COUNT(pt.track_id) AS song_count
      FROM playlists p
      JOIN playlists_import pt ON p.playlist_id = pt.playlist_id
      JOIN tracks_import t ON pt.track_id = t.track_id
      GROUP BY p.playlist_id, p.name, p.followers
    )
    SELECT pf.playlist_id, pf.name, pf.followers, pf.song_count,
           pf.avg_danceability, pf.avg_energy, pf.avg_liveness,
           pf.avg_key, pf.avg_loudness, pf.avg_speechiness,
           pf.avg_acousticness, pf.avg_valence, pf.avg_tempo
    FROM playlist_features pf
    WHERE ${moodConditions[mood]}
    ORDER BY pf.followers DESC
    LIMIT $1;
  `;

  try {
    const result = await connection.query(query, [limit]);
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Database query failed' });
  }
};

module.exports = {
  home,
  top_songs,
  top_albums,
  top_playlists,
  search_songs,
  search_albums,
  search_playlists,
  recommend_song_on_song,
  recommend_song_on_artist,
  recommend_song_on_playlist,
  recommend_playlist_on_song,
  recommend_artists_by_similarity,
  artist_stats,
  recommend_playlists_by_mood,
};
