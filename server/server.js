const express = require('express');
const cors = require('cors');
const config = require('./config');
const routes = require('./routes');

const app = express();
app.use(cors({
  origin: '*',
}));

app.get('/home', routes.home);
app.get('/top_songs', routes.top_songs);
app.get('/top_albums', routes.top_albums);
app.get('/top_playlists', routes.top_playlists);

app.get('/search_songs', routes.search_songs);
app.get('/search_albums', routes.search_albums);
app.get('/search_playlists', routes.search_playlists);

app.get('/recommend_song_on_song', routes.recommend_song_on_song);
app.get('/recommend_song_on_artist', routes.recommend_song_on_artist);
app.get('/recommend_song_on_playlist', routes.recommend_song_on_playlist);
app.get('/recommend_playlist_on_song', routes.recommend_playlist_on_song);
app.get('/recommend_artists_by_similarity', routes.recommend_artists_by_similarity);

app.get('/artist_stats', routes.artist_stats);
app.get('/happiest_artists', routes.happiest_artists);

app.listen(config.server_port, () => {
  console.log(`Server running at http://${config.server_host}:${config.server_port}/`)
});

module.exports = app;
