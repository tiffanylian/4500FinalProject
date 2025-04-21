const { Pool, types } = require('pg');
const config = require('./config.json');

// Override the default parsing for BIGINT
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
    authors: [
      'Arriella Mafuta',
      'Xiang Chen',
      'Tiffany Lian',
      'Lucas Lee'
    ],
    description: 'Spotify Recommendation App with advanced filtering and recommendation features.'
  });
};

const top_songs = async (req, res) => {
  const limit = parseInt(req.query.limit) || 20;
  const page = parseInt(req.query.page) || 1;
  const offset = (page - 1) * limit;
  const sort_by = req.query.sort_by || 'popularity';
  const year = req.query.year;

  let query;
  let values;

  if (year) {
    query = `
      SELECT t.name AS track_name, a.name AS artist_name, t.popularity, t.year
      FROM tracks t JOIN artists a ON t.artist_id = a.artist_id
      WHERE t.year = $1 AND t.popularity IS NOT NULL
      ORDER BY ${sort_by} DESC
      LIMIT $2 OFFSET $3
    `;
    values = [year, limit, offset];
  } else {
    query = `
      SELECT t.name AS track_name, a.name AS artist_name, t.popularity, t.year
      FROM tracks t JOIN artists a ON t.artist_id = a.artist_id
      WHERE t.popularity IS NOT NULL
      ORDER BY ${sort_by} DESC
      LIMIT $1 OFFSET $2
    `;
    values = [limit, offset];
  }
  

  connection.query(query, values, (err, data) => {
    if (err) {
      console.error(err);
      res.status(500).json({ error: 'Database query failed' });
    } else {
      res.json(data.rows);
    }
  });
};

const top_albums = async (req, res) => {
  const limit = parseInt(req.query.limit) || 20;
  const page = parseInt(req.query.page) || 1;
  const offset = (page - 1) * limit;
  const sort_by = req.query.sort_by || 'avg_popularity';

  connection.query(`
    SELECT al.name AS album_name, al.album_id, ar.name AS artist_name,
           ROUND(AVG(t.popularity), 2) AS avg_popularity
    FROM albums al
    JOIN tracks t ON al.album_id = t.album_id
    JOIN artists ar ON al.artist_id = ar.artist_id
    GROUP BY al.album_id, al.name, ar.name
    HAVING COUNT(t.popularity) > 0
    ORDER BY ${sort_by} DESC
    LIMIT $1 OFFSET $2
  `, [limit, offset], (err, data) => {
    if (err) {
      console.error(err);
      res.json([]);
    } else {
      res.json(data.rows);
    }
  });  
};

const top_playlists = async (req, res) => {
  const limit = parseInt(req.query.limit) || 20;
  const page = parseInt(req.query.page) || 1;
  const offset = (page - 1) * limit;
  const sort_by = req.query.sort_by || 'followers';

  connection.query(`
    SELECT playlist_id, name, followers
    FROM playlists
    ORDER BY ${sort_by} DESC
    LIMIT $1 OFFSET $2
  `, [limit, offset], (err, data) => {
    if (err) {
      console.error(err);
      res.json([]);
    } else {
      res.json(data.rows);
    }
  });
};

const search_songs = async (req, res) => {
  const {
    track_name = '',
    explicit,
    sort_by = 'popularity',
    limit = 20,
    offset = 0,
    year,
  } = req.query;

  const conditions = [];
  const values = [];

  if (track_name) {
    conditions.push(`t.name ILIKE $${values.length + 1}`);
    values.push(`%${track_name}%`);
  }
  if (explicit !== undefined) {
    conditions.push(`t.explicit = $${values.length + 1}`);
    values.push(explicit === 'true' ? 1 : 0);
  }
  if (year) {
    conditions.push(`t.year = $${values.length + 1}`);
    values.push(year);
  }

  const whereClause = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';

  const query = `
    SELECT t.name AS track_name, a.name AS artist_name, t.year
    FROM tracks t JOIN artists a ON t.artist_id = a.artist_id
    ${whereClause}
    ORDER BY ${sort_by} DESC
    LIMIT $${values.length + 1} OFFSET $${values.length + 2}
  `;
  values.push(parseInt(limit));
  values.push(parseInt(offset));

  connection.query(query, values, (err, data) => {
    if (err) {
      console.error(err);
      res.json([]);
    } else {
      res.json(data.rows);
    }
  });
};

const search_albums = async (req, res) => {
  const { album_name = '', sort_by = 'avg_popularity', limit = 20, offset = 0 } = req.query;

  const nameFilter = `%${album_name}%`;

  connection.query(`
    SELECT 
      al.name AS album_name,
      ar.name AS artist_name,
      ARRAY_AGG(t.name) AS songs, 
      ROUND(AVG(t.popularity) FILTER (WHERE t.popularity IS NOT NULL), 2) AS avg_popularity
    FROM albums al
    JOIN artists ar ON al.artist_id = ar.artist_id
    JOIN tracks t ON al.album_id = t.album_id
    WHERE al.name ILIKE $1
    GROUP BY al.album_id, ar.name
    HAVING COUNT(t.popularity) > 0
    ORDER BY ${sort_by} DESC
    LIMIT $2 OFFSET $3
  `, [nameFilter, limit, offset], (err, data) => {
    if (err) {
      console.error(err);
      res.json([]);
    } else {
      res.json(data.rows);
    }
  });
};



const search_playlists = async (req, res) => {
  const { playlist_name = '', sort_by = 'followers', limit = 20, offset = 0 } = req.query;

  const nameFilter = `%${playlist_name}%`;

  connection.query(`
    SELECT p.name AS playlist_name, COUNT(pt.track_id) AS song_count, p.followers
    FROM playlists p
    JOIN playlist_track pt ON p.playlist_id = pt.playlist_id
    WHERE p.name ILIKE $1
    GROUP BY p.playlist_id
    ORDER BY ${sort_by} DESC
    LIMIT $2 OFFSET $3
  `, [nameFilter, limit, offset], (err, data) => {
    if (err) {
      console.error(err);
      res.json([]);
    } else {
      res.json(data.rows);
    }
  });
};


const recommend_song_on_song = async (req, res) => {
  const { track_id, limit = 20, offset = 0 } = req.query;

  connection.query(`
    SELECT t2.name AS track_name, a.name AS artist_name, t2.popularity, t2.year
    FROM tracks t1
    JOIN tracks t2 ON t1.track_id != t2.track_id
    JOIN artists a ON t2.artist_id = a.artist_id
    WHERE t1.track_id = $1
    ORDER BY ((t1.energy * t2.energy + t1.danceability * t2.danceability)) DESC
    LIMIT $2 OFFSET $3
  `, [track_id, limit, offset], (err, data) => {
    if (err) {
      console.error(err);
      res.json([]);
    } else {
      res.json(data.rows);
    }
  });
};

const recommend_song_on_artist = async (req, res) => {
  const artistId = req.query.artist_id;
  const limit = parseInt(req.query.limit) || 20;
  const offset = parseInt(req.query.offset) || 0;

  if (!artistId) {
    return res.status(400).json({ error: 'Missing artist_id' });
  }

  try {
    // Step 1: get average danceability and energy for artist's songs
    const avgFeaturesQuery = `
      SELECT 
        AVG(danceability) AS danceability,
        AVG(energy) AS energy
      FROM tracks
      WHERE artist_id = $1
    `;
    const avgResult = await connection.query(avgFeaturesQuery, [artistId]);
    const artistVector = avgResult.rows[0];

    // Step 2: get most common genre of the artist
    const genreQuery = `
      SELECT genre
      FROM tracks
      WHERE artist_id = $1 AND genre IS NOT NULL
      GROUP BY genre
      ORDER BY COUNT(*) DESC
      LIMIT 1
    `;
    const genreResult = await connection.query(genreQuery, [artistId]);
    const topGenre = genreResult.rows[0]?.genre;

    if (!artistVector || !topGenre) {
      return res.json([]);
    }

    // Step 3: cosine similarity with other tracks of same genre and not by the same artist
    const similarityQuery = `
      SELECT t.track_id, t.name AS track_name, a.name AS artist_name, t.popularity, t.year,
             (t.danceability * $1 + t.energy * $2) / (SQRT(t.danceability^2 + t.energy^2) * SQRT($1^2 + $2^2)) AS similarity
      FROM tracks t
      JOIN artists a ON t.artist_id = a.artist_id
      WHERE t.genre = $3 AND t.artist_id != $4
        AND t.danceability IS NOT NULL AND t.energy IS NOT NULL
      ORDER BY similarity DESC
      LIMIT $5 OFFSET $6
    `;
    const simResult = await connection.query(similarityQuery, [
      artistVector.danceability,
      artistVector.energy,
      topGenre,
      artistId,
      limit,
      offset
    ]);

    res.json(simResult.rows);

  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal server error' });
  }
};

const recommend_song_on_playlist = async (req, res) => {
  const playlist_id = req.query.playlist_id;
  const limit = parseInt(req.query.limit) || 20;
  const offset = parseInt(req.query.offset) || 0;

  if (!playlist_id) {
    return res.status(400).json({ error: 'playlist_id is required' });
  }

  // Step 1: Get average danceability, energy and top genre of the playlist
  const avgQuery = `
    SELECT 
      AVG(t.danceability) AS avg_danceability,
      AVG(t.energy) AS avg_energy,
      (
        SELECT genre
        FROM tracks t2
        JOIN playlist_track pt2 ON t2.track_id = pt2.track_id
        WHERE pt2.playlist_id = $1 AND t2.genre IS NOT NULL
        GROUP BY genre
        ORDER BY COUNT(*) DESC
        LIMIT 1
      ) AS top_genre
    FROM tracks t
    JOIN playlist_track pt ON t.track_id = pt.track_id
    WHERE pt.playlist_id = $1
  `;

  connection.query(avgQuery, [playlist_id], (err, result) => {
    if (err) {
      console.error(err);
      return res.status(500).json({ error: 'Database query failed' });
    }

    const { avg_danceability, avg_energy, top_genre } = result.rows[0];

    if (avg_danceability == null || avg_energy == null || !top_genre) {
      return res.status(404).json({ error: 'Playlist data not found or invalid' });
    }

    // Step 2: Recommend songs similar to the average vector, in the same genre and not in the playlist
    const recQuery = `
      SELECT 
        t.track_id,
        t.name AS track_name,
        a.name AS artist_name,
        t.year,
        t.popularity,
        t.danceability,
        t.energy,
        (t.danceability * $2 + t.energy * $3) / 
        (SQRT(POWER(t.danceability, 2) + POWER(t.energy, 2)) * SQRT(POWER($2, 2) + POWER($3, 2))) AS cosine_similarity
      FROM tracks t
      JOIN artists a ON t.artist_id = a.artist_id
      WHERE t.genre = $1
        AND t.track_id NOT IN (
          SELECT track_id FROM playlist_track WHERE playlist_id = $4
        )
        AND t.danceability IS NOT NULL
        AND t.energy IS NOT NULL
      ORDER BY cosine_similarity DESC
      LIMIT $5 OFFSET $6
    `;

    const values = [
      top_genre,
      avg_danceability,
      avg_energy,
      playlist_id,
      limit,
      offset,
    ];

    connection.query(recQuery, values, (err2, data) => {
      if (err2) {
        console.error(err2);
        return res.status(500).json({ error: 'Failed to generate recommendations' });
      }

      res.json(data.rows);
    });
  });
};

const underrated_tracks = async function (req, res) {
  const popularity_threshold = req.query.popularity_threshold ?? 30;
  const min_energy = req.query.min_energy ?? 0.7;
  const min_danceability = req.query.min_danceability ?? 0.7;
  const limit = req.query.limit ?? 20;

  const query = `
    SELECT T.name AS track_name, A.name AS artist_name, T.popularity, T.energy, T.danceability
    FROM tracks T
    JOIN artists A ON T.artist_id = A.artist_id
    WHERE T.popularity <= $1
      AND T.energy >= $2
      AND T.danceability >= $3
    ORDER BY T.popularity ASC
    LIMIT $4;
  `;

  try {
    const { rows } = await connection.query(query, [
      popularity_threshold,
      min_energy,
      min_danceability,
      limit
    ]);
    res.json(rows);
  } catch (err) {
    console.error(err);
    res.status(500).json([]);
  }
};

const artist_stats = async function(req, res) {
  const artistId = req.query.artist_id;
  if (!artistId) return res.status(400).json({ error: 'artist_id required' });

  try {
    const artistNameQuery = `
      SELECT name FROM artists WHERE artist_id = $1
    `;
    const topSongQuery = `
      SELECT name AS track_name, popularity, release_date
      FROM tracks
      WHERE artist_id = $1
      ORDER BY popularity DESC
      LIMIT 1
    `;
    const avgPopularityQuery = `
      SELECT AVG(popularity) AS avg_popularity
      FROM tracks
      WHERE artist_id = $1
    `;
    const popByYearQuery = `
      SELECT year, COUNT(*) AS num_tracks, AVG(popularity) AS avg_popularity
      FROM tracks
      WHERE artist_id = $1
      GROUP BY year ORDER BY year
    `;
    const genreDistQuery = `
      SELECT genre, COUNT(*) AS track_count
      FROM tracks
      WHERE artist_id = $1
      GROUP BY genre ORDER BY track_count DESC
    `;

    const artist = await connection.query(artistNameQuery, [artistId]);
    const topSong = await connection.query(topSongQuery, [artistId]);
    const avgPopularity = await connection.query(avgPopularityQuery, [artistId]);
    const popByYear = await connection.query(popByYearQuery, [artistId]);
    const genreDist = await connection.query(genreDistQuery, [artistId]);

    res.json({
      artist_name: artist.rows[0].name,
      avg_popularity: parseFloat(avgPopularity.rows[0].avg_popularity),
      top_song: topSong.rows[0],
      popularity_by_year: popByYear.rows,
      genre_distribution: genreDist.rows,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal server error' });
  }
};

const trending_artists = async function(req, res) {
  const startYear = req.query.start_year;
  const endYear = req.query.end_year;
  const limit = req.query.limit ?? 10;

  if (!startYear || !endYear) {
    return res.status(400).json({ error: 'start_year and end_year are required' });
  }

  const query = `
WITH artist_start AS (
  SELECT artist_id, AVG(popularity) AS start_avg
  FROM tracks
  WHERE year = $1 AND popularity IS NOT NULL
  GROUP BY artist_id
),
artist_end AS (
  SELECT artist_id, AVG(popularity) AS end_avg
  FROM tracks
  WHERE year = $2 AND popularity IS NOT NULL
  GROUP BY artist_id
),
popularity_growth AS (
  SELECT e.artist_id, (e.end_avg - s.start_avg) AS growth, s.start_avg, e.end_avg
  FROM artist_start s
  JOIN artist_end e ON s.artist_id = e.artist_id
)
SELECT a.name AS artist_name, p.growth AS popularity_growth, p.start_avg, p.end_avg
FROM popularity_growth p
JOIN artists a ON a.artist_id = p.artist_id
WHERE p.start_avg IS NOT NULL AND p.end_avg IS NOT NULL
ORDER BY p.growth DESC
LIMIT $3;
  `;

  connection.query(query, [startYear, endYear, limit], (err, data) => {
    if (err) {
      console.error(err);
      res.status(500).json({ error: 'Internal server error' });
    } else {
      res.json(data.rows);
    }
  });
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
  underrated_tracks,
  artist_stats,
  trending_artists
};
