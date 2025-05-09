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

const resolve_name_to_id = async (name, type) => {
  let query, idField;
  
  if (type === 'artist') {
    query = `SELECT artist_id FROM artists WHERE name ILIKE $1 LIMIT 1`;
    idField = 'artist_id';
  } else if (type === 'song') {
    query = `SELECT track_id FROM tracks_import WHERE name ILIKE $1 LIMIT 1`;
    idField = 'track_id';
  } else if (type === 'playlist') {
    query = `SELECT playlist_id FROM playlists WHERE name ILIKE $1 LIMIT 1`;
    idField = 'playlist_id';
  } else {
    throw new Error('Invalid type');
  }

  const result = await connection.query(query, [`%${name}%`]);
  if (result.rows.length === 0) {
    throw new Error('No match found');
  }

  return result.rows[0][idField];
};

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
  let paramIndex = 1;

  let query = `
    SELECT track_id, 
           track_name, 
           artists, 
           playlist_count
    FROM mv_top_songs
  `;

  if (year) {
    query += ` WHERE year = $${paramIndex}`;
    values.push(year);
    paramIndex++;
  }

  query += `
    ORDER BY playlist_count DESC
    LIMIT $${paramIndex}
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
  let paramIndex = 1;

  let query = `
    SELECT album_id, album_name, artist_name, playlist_count, year
    FROM mv_top_albums
  `;

  if (year) {
    query += ` WHERE year = $${paramIndex}`;
    values.push(year);
    paramIndex++;
  }

  query += `
    ORDER BY playlist_count DESC
    LIMIT $${paramIndex}
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
    conditions.push(`track_name ILIKE $${values.length + 1}`);
    values.push(`%${name}%`);
  }
  if (artist) {
    conditions.push(`artist_name ILIKE $${values.length + 1}`);
    values.push(`%${artist}%`);
  }
  if (year) {
    conditions.push(`year = $${values.length + 1}`);
    values.push(year);
  }

  const whereClause = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
  const query = `
    SELECT track_id, track_name, artist_name, playlist_count
    FROM mv_search_songs
    ${whereClause}
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
    conditions.push(`album_name ILIKE $${values.length + 1}`);
    values.push(`%${name}%`);
  }
  if (artist) {
    conditions.push(`artist_name ILIKE $${values.length + 1}`);
    values.push(`%${artist}%`);
  }

  const whereClause = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
  const query = `
    SELECT album_id, album_name, artist_name, playlist_count
    FROM mv_search_albums
    ${whereClause}
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
    SELECT playlist_id, name, song_count, followers
    FROM v_search_playlists
    WHERE name ILIKE $1
    ORDER BY followers DESC
    LIMIT $2
  `;

  connection.query(query, [nameFilter, limit], (err, data) => {
    if (err) res.status(500).json({ error: 'Database query failed' });
    else res.json(data.rows);
  });
};

const recommend_song_on_song = async (req, res) => {
  let { track_id, name, limit = 10 } = req.query;

  try {
    if (!track_id && name) {
      track_id = await resolve_name_to_id(name, 'song');
      if (!track_id) {
        return res.status(404).json({ error: 'Song not found' });
      }
    }

    const query = `
WITH t1 AS (
  SELECT *, SQRT(POWER(danceability,2) + POWER(energy,2) + POWER(liveness,2) +
                 POWER(key,2) + POWER(loudness,2) + POWER(speechiness,2) +
                 POWER(acousticness,2) + POWER(valence,2) + POWER(tempo,2)) AS norm
  FROM tracks_import
  WHERE track_id = $1
),
playlist_candidates AS (
  SELECT DISTINCT pt2.track_id
  FROM playlists_import pt1
  JOIN playlists_import pt2 ON pt1.playlist_id = pt2.playlist_id
  WHERE pt1.track_id = $1 AND pt2.track_id <> $1
),
t2 AS (
  SELECT t.track_id, t.name, a.name AS artist_name,
         t.danceability, t.energy, t.liveness, t.key, t.loudness,
         t.speechiness, t.acousticness, t.valence, t.tempo,
         SQRT(POWER(t.danceability,2) + POWER(t.energy,2) + POWER(t.liveness,2) +
              POWER(t.key,2) + POWER(t.loudness,2) + POWER(t.speechiness,2) +
              POWER(t.acousticness,2) + POWER(t.valence,2) + POWER(t.tempo,2)) AS norm
  FROM tracks_import t
  JOIN artists a ON t.artist_id = a.artist_id
  WHERE t.track_id IN (SELECT track_id FROM playlist_candidates)
)
SELECT t2.track_id, t2.name,
       STRING_AGG(t2.artist_name, ', ') AS artist_names,
       (t1.danceability * t2.danceability +
        t1.energy * t2.energy +
        t1.liveness * t2.liveness +
        t1.key * t2.key +
        t1.loudness * t2.loudness +
        t1.speechiness * t2.speechiness +
        t1.acousticness * t2.acousticness +
        t1.valence * t2.valence +
        t1.tempo * t2.tempo) / (NULLIF(t1.norm, 0) * NULLIF(t2.norm, 0)) AS similarity
FROM t1, t2
GROUP BY t2.track_id, t2.name, t1.danceability, t1.energy, t1.liveness, t1.key,
         t1.loudness, t1.speechiness, t1.acousticness, t1.valence, t1.tempo,
         t1.norm, t2.danceability, t2.energy, t2.liveness, t2.key, t2.loudness,
         t2.speechiness, t2.acousticness, t2.valence, t2.tempo, t2.norm
ORDER BY similarity DESC
LIMIT $2;

    `;

    connection.query(query, [track_id, limit], (err, data) => {
      if (err) res.status(500).json({ error: 'Database query failed' });
      else res.json(data.rows);
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to resolve song name or run query' });
  }
};

const recommend_song_on_artist = async (req, res) => {
  let { artist_id, name, limit = 10 } = req.query;

  try {
    // Resolve artist_id if only name is provided
    if (!artist_id && name) {
      artist_id = await resolve_name_to_id(name, 'artist');
      if (!artist_id) {
        return res.status(404).json({ error: 'Artist not found' });
      }
    }

    const query = `
      WITH aa AS (
        SELECT artist_id, danceability, energy, liveness, key, loudness,
               speechiness, acousticness, valence, tempo,
               SQRT(POWER(danceability,2) + POWER(energy,2) + POWER(liveness,2) +
                    POWER(key,2) + POWER(loudness,2) + POWER(speechiness,2) +
                    POWER(acousticness,2) + POWER(valence,2) + POWER(tempo,2)) AS norm
        FROM mv_artist_avg_attributes
        WHERE artist_id = $1
      ),
      t AS (
        SELECT ti.*, a.name AS artist_name,
               SQRT(POWER(ti.danceability,2) + POWER(ti.energy,2) + POWER(ti.liveness,2) +
                    POWER(ti.key,2) + POWER(ti.loudness,2) + POWER(ti.speechiness,2) +
                    POWER(ti.acousticness,2) + POWER(ti.valence,2) + POWER(ti.tempo,2)) AS norm
        FROM tracks_import ti
        JOIN artists a ON ti.artist_id = a.artist_id
        WHERE ti.artist_id <> $1
      )
      SELECT t.track_id, t.name,
             STRING_AGG(t.artist_name, ', ') AS artist_names,
             (t.danceability * aa.danceability +
              t.energy * aa.energy +
              t.liveness * aa.liveness +
              t.key * aa.key +
              t.loudness * aa.loudness +
              t.speechiness * aa.speechiness +
              t.acousticness * aa.acousticness +
              t.valence * aa.valence +
              t.tempo * aa.tempo) / (NULLIF(t.norm, 0) * NULLIF(aa.norm, 0)) AS similarity
      FROM t, aa
      GROUP BY t.track_id, t.name, t.danceability, t.energy, t.liveness, t.key,
               t.loudness, t.speechiness, t.acousticness, t.valence, t.tempo, t.norm,
               aa.danceability, aa.energy, aa.liveness, aa.key, aa.loudness, aa.speechiness,
               aa.acousticness, aa.valence, aa.tempo, aa.norm
      ORDER BY similarity DESC
      LIMIT $2;
    `;

    const values = [artist_id, limit];

    const result = await connection.query(query, values);
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to resolve artist name or run query' });
  }
};

const recommend_song_on_playlist = async (req, res) => {
  let { playlist_id, name, limit = 10 } = req.query;

  try {
    if (!playlist_id && name) {
      playlist_id = await resolve_name_to_id(name, 'playlist');
      if (!playlist_id) {
        return res.status(404).json({ error: 'Playlist not found' });
      }
    }

    const query = `
      WITH playlist_avg AS (
        SELECT *
        FROM mv_playlist_avg_attributes
        WHERE playlist_id = $1
      )
      SELECT t.track_id,
             t.name,
             STRING_AGG(DISTINCT a.name, ', ') AS artist_names,
        (t.danceability * pa.avg_danceability +
         t.energy * pa.avg_energy +
         t.liveness * pa.avg_liveness +
         t.key * pa.avg_key +
         t.loudness * pa.avg_loudness +
         t.speechiness * pa.avg_speechiness +
         t.acousticness * pa.avg_acousticness +
         t.valence * pa.avg_valence +
         t.tempo * pa.avg_tempo) /
        (NULLIF(SQRT(POWER(t.danceability,2) + POWER(t.energy,2) + POWER(t.liveness,2) +
                    POWER(t.key,2) + POWER(t.loudness,2) + POWER(t.speechiness,2) +
                    POWER(t.acousticness,2) + POWER(t.valence,2) + POWER(t.tempo,2)), 0) *
         NULLIF(SQRT(POWER(pa.avg_danceability,2) + POWER(pa.avg_energy,2) + POWER(pa.avg_liveness,2) +
                    POWER(pa.avg_key,2) + POWER(pa.avg_loudness,2) + POWER(pa.avg_speechiness,2) +
                    POWER(pa.avg_acousticness,2) + POWER(pa.avg_valence,2) + POWER(pa.avg_tempo,2)), 0)) AS similarity
      FROM tracks_import t
      JOIN artists a ON t.artist_id = a.artist_id
      CROSS JOIN playlist_avg pa
      WHERE t.track_id NOT IN (
        SELECT track_id FROM playlists_import WHERE playlist_id = $1
      )
      GROUP BY t.track_id, t.name, pa.avg_danceability, pa.avg_energy, pa.avg_liveness,
               pa.avg_key, pa.avg_loudness, pa.avg_speechiness, pa.avg_acousticness,
               pa.avg_valence, pa.avg_tempo,
               t.danceability, t.energy, t.liveness, t.key, t.loudness, t.speechiness,
               t.acousticness, t.valence, t.tempo
      ORDER BY similarity DESC
      LIMIT $2;
    `;

    const values = [playlist_id, limit];
    const result = await connection.query(query, values);
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to resolve playlist name or run query' });
  }
};

const recommend_playlist_on_song = async (req, res) => {
  let { track_id, name, limit = 10 } = req.query;

  try {
    if (!track_id && name) {
      track_id = await resolve_name_to_id(name, 'song');
      if (!track_id) return res.status(404).json({ error: 'Song not found' });
    }

    const query = `
      WITH song_attributes AS (
        SELECT danceability, energy, liveness, key, loudness,
               speechiness, acousticness, valence, tempo,
               SQRT(POWER(danceability,2) + POWER(energy,2) + POWER(liveness,2) +
                    POWER(key,2) + POWER(loudness,2) + POWER(speechiness,2) +
                    POWER(acousticness,2) + POWER(valence,2) + POWER(tempo,2)) AS norm
        FROM tracks_import
        WHERE track_id = $1
      )
      SELECT pa.playlist_id, pa.name,
        (pa.avg_danceability * sa.danceability +
         pa.avg_energy * sa.energy +
         pa.avg_liveness * sa.liveness +
         pa.avg_key * sa.key +
         pa.avg_loudness * sa.loudness +
         pa.avg_speechiness * sa.speechiness +
         pa.avg_acousticness * sa.acousticness +
         pa.avg_valence * sa.valence +
         pa.avg_tempo * sa.tempo) /
        (NULLIF(SQRT(POWER(pa.avg_danceability,2) + POWER(pa.avg_energy,2) + POWER(pa.avg_liveness,2) +
                    POWER(pa.avg_key,2) + POWER(pa.avg_loudness,2) + POWER(pa.avg_speechiness,2) +
                    POWER(pa.avg_acousticness,2) + POWER(pa.avg_valence,2) + POWER(pa.avg_tempo,2)), 0) *
         NULLIF(sa.norm, 0)) AS similarity
      FROM mv_playlist_avg_attributes pa, song_attributes sa
      WHERE NOT EXISTS (
        SELECT 1
        FROM playlists_import pt_check
        WHERE pt_check.playlist_id = pa.playlist_id
          AND pt_check.track_id = $1
      )
      ORDER BY similarity DESC
      LIMIT $2;
    `;

    const result = await connection.query(query, [track_id, limit]);
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to resolve song name or run query' });
  }
};

const recommend_artists_by_similarity = async (req, res) => {
  let { artist_id, name, limit = 10 } = req.query;

  try {
    if (!artist_id && name) {
      artist_id = await resolve_name_to_id(name, 'artist');
      if (!artist_id) return res.status(404).json({ error: 'Artist not found' });
    }

    const query = `
SELECT a.artist_id, ar.name,
  (a.danceability * t.danceability +
   a.energy * t.energy +
   a.liveness * t.liveness +
   a.key * t.key +
   a.loudness * t.loudness +
   a.speechiness * t.speechiness +
   a.acousticness * t.acousticness +
   a.valence * t.valence +
   a.tempo * t.tempo) /
  (NULLIF(SQRT(POWER(a.danceability,2) + POWER(a.energy,2) + POWER(a.liveness,2) +
              POWER(a.key,2) + POWER(a.loudness,2) + POWER(a.speechiness,2) +
              POWER(a.acousticness,2) + POWER(a.valence,2) + POWER(a.tempo,2)),0) *
   NULLIF(SQRT(POWER(t.danceability,2) + POWER(t.energy,2) + POWER(t.liveness,2) +
              POWER(t.key,2) + POWER(t.loudness,2) + POWER(t.speechiness,2) +
              POWER(t.acousticness,2) + POWER(t.valence,2) + POWER(t.tempo,2)),0)) AS similarity
FROM mv_artist_avg_attributes a
JOIN mv_artist_avg_attributes t ON t.artist_id = $1
JOIN artists ar ON a.artist_id = ar.artist_id
WHERE a.artist_id <> $1
ORDER BY similarity DESC
LIMIT $2;
    `;

    const result = await connection.query(query, [artist_id, limit]);
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to resolve artist name or run query' });
  }
};

const artist_stats = async (req, res) => {
  let { artist_id, name } = req.query;
  if (!artist_id && name) {
    artist_id = await resolve_name_to_id(name, 'artist');
    if (!artist_id) return res.status(404).json({ error: 'Artist not found' });
  }

  const query = `
    WITH track_stats AS (
  SELECT
    t.artist_id,
    t.track_id,
    t.name AS track_name,
    t.year,
    t.danceability,
    t.energy,
    t.valence,
    t.acousticness,
    t.loudness,
    COUNT(pt.playlist_id) AS playlist_count
  FROM tracks_import t
  LEFT JOIN playlists_import pt ON t.track_id = pt.track_id
  WHERE t.artist_id = $1
  GROUP BY t.artist_id, t.track_id, t.name, t.year, t.danceability, t.energy, t.valence, t.acousticness, t.loudness
),
top_song AS (
  SELECT track_id, track_name, playlist_count
  FROM track_stats
  ORDER BY playlist_count DESC
  LIMIT 1
),
stats_by_year AS (
  SELECT year,
         AVG(danceability) AS avg_danceability,
         AVG(energy) AS avg_energy,
         AVG(valence) AS avg_valence,
         AVG(acousticness) AS avg_acousticness,
         AVG(loudness) AS avg_loudness,
         SUM(playlist_count) AS playlist_count
  FROM track_stats
  GROUP BY year
)
SELECT a.artist_id, a.name AS artist_name,
       (SELECT json_build_object('track_id', ts.track_id, 'name', ts.track_name, 'playlist_count', ts.playlist_count)
        FROM top_song ts) AS top_song,
       (SELECT json_agg(sy) FROM stats_by_year sy) AS stats_by_year
FROM artists a
WHERE a.artist_id = $1;
  `;

  try {
    const result = await connection.query(query, [artist_id]);
    if (result.rowCount === 0) {
      return res.status(404).json({ error: 'Artist not found' });
    }
    res.json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to resolve artist name or run query' });
  }
};

const happiest_artists = async (req, res) => {
  // allow client to override how many artists to return
  const limit = parseInt(req.query.limit, 10) || 5;

  const query = `
    WITH
      track_playlist_counts AS (
        SELECT
          ti.track_id,
          ti.artist_id,
          COUNT(DISTINCT pi.playlist_id) AS playlist_count
        FROM tracks_import ti
        LEFT JOIN playlists_import pi
          ON ti.track_id  = pi.track_id
         AND ti.artist_id = pi.artist_id
        GROUP BY ti.track_id, ti.artist_id
      ),

      artist_stats AS (
        SELECT
          ti.artist_id,
          COUNT(DISTINCT ti.track_id) AS total_tracks
        FROM tracks_import ti
        LEFT JOIN track_playlist_counts tpc
          USING(track_id, artist_id)
        GROUP BY ti.artist_id
      ),

      artist_album_valence AS (
        SELECT
          ti.artist_id,
          al.album_id,
          al.name       AS album_name,
          AVG(ti.valence) AS avg_valence,
          RANK() OVER (
            PARTITION BY ti.artist_id
            ORDER BY AVG(ti.valence) DESC
          ) AS valence_rank
        FROM tracks_import ti
        JOIN albums al
          ON ti.album_id = al.album_id
        GROUP BY ti.artist_id, al.album_id, al.name
      ),

      artist_top_album AS (
        SELECT
          artist_id,
          album_id,
          album_name,
          avg_valence
        FROM artist_album_valence
        WHERE valence_rank = 1
      )

    SELECT
      a.artist_id,
      a.name                    AS artist_name,
      ast.total_tracks,
      ata.album_id,
      ata.album_name,
      ROUND(ata.avg_valence::numeric, 3) AS top_album_valence
    FROM artist_stats ast
    JOIN artists a
      ON ast.artist_id = a.artist_id
    LEFT JOIN artist_top_album ata
      ON ast.artist_id = ata.artist_id
    ORDER BY top_album_valence DESC
    LIMIT $1;
  `;

  try {
    const result = await connection.query(query, [limit]);
    // if you want a more consistent shape with artist_stats:
    res.json(result.rows);
  } catch (err) {
    console.error('Error fetching artist insights', err);
    res.status(500).json({
      success: false,
      error:   'Internal server error'
    });
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
  happiest_artists
};
