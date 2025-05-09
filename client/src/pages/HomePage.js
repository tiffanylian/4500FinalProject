import React, { useState, useEffect } from 'react';
import { Link as RouterLink } from 'react-router-dom';
import {
  Container,
  Grid,
  Box,
  Card,
  CardHeader,
  CardContent,
  Typography,
  Button
} from '@mui/material';

export default function HomePage() {
  const [topSongs, setTopSongs] = useState([]);
  const [topAlbums, setTopAlbums] = useState([]);
  const [topPlaylists, setTopPlaylists] = useState([]);

  useEffect(() => {
    fetch('/top_songs')
      .then(res => res.json())
      .then(data => setTopSongs(data))
      .catch(console.error);

    fetch('/top_albums')
      .then(res => res.json())
      .then(data => setTopAlbums(data))
      .catch(console.error);

    fetch('/top_playlists')
      .then(res => res.json())
      .then(data => setTopPlaylists(data))
      .catch(console.error);
  }, []);

  return (
    <Container maxWidth="lg" sx={{ py: 4 }}>
      {/* Header */}
      <Box textAlign="center" mb={6}>
        <Typography variant="h3" component="h1" gutterBottom>
          SpotRec
        </Typography>
        <Typography variant="subtitle1" color="green">
          Your personalized Spotify-inspired music recommendation hub
        </Typography>
      </Box>

      {/* Top Sections */}
      <Grid container spacing={4} mb={6}>
        {/* Top Songs */}
        <Grid item xs={12} md={4}>
          <Card elevation={3}>
            <CardHeader title="Top Songs" />
            <CardContent>
              {topSongs.slice(0, 5).map(song => (
                <Typography key={song.id} variant="body2">
                  {song.track_name} — {song.artists}
                </Typography>
              ))}
              <Box mt={2}>
                <Button
                  component={RouterLink}
                  to="/top_songs"
                  variant="outlined"
                  fullWidth
                >
                  See All Songs
                </Button>
              </Box>
            </CardContent>
          </Card>
        </Grid>

        {/* Top Albums */}
        <Grid item xs={12} md={4}>
          <Card elevation={3}>
            <CardHeader title="Top Albums" />
            <CardContent>
              {topAlbums.slice(0, 5).map(album => (
                <Typography key={album.id} variant="body2">
                  {album.album_name} — {album.artists}
                </Typography>
              ))}
              <Box mt={2}>
                <Button
                  component={RouterLink}
                  to="/top_albums"
                  variant="outlined"
                  fullWidth
                >
                  See All Albums
                </Button>
              </Box>
            </CardContent>
          </Card>
        </Grid>

        {/* Top Playlists */}
        <Grid item xs={12} md={4}>
          <Card elevation={3}>
            <CardHeader title="Top Playlists" />
            <CardContent>
              {topPlaylists.slice(0, 5).map(list => (
                <Typography key={list.id} variant="body2">
                  {list.name}
                </Typography>
              ))}
              <Box mt={2}>
                <Button
                  component={RouterLink}
                  to="/top_playlists"
                  variant="outlined"
                  fullWidth
                >
                  See All Playlists
                </Button>
              </Box>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Features */}
      <Box mb={4}>
        <Typography variant="h5" gutterBottom>
          Explore Features
        </Typography>
        <Grid container spacing={2}>
          {[
            { label: 'Search Songs', path: '/search_songs' },
            { label: 'Search Albums', path: '/search_albums' },
            { label: 'Search Playlists', path: '/search_playlists' },
            { label: 'Recommend on Song', path: '/recommend_song_on_song' },
            { label: 'Recommend on Artist', path: '/recommend_song_on_artist' },
            { label: 'Song → Playlist Rec', path: '/recommend_song_on_playlist' },
            { label: 'Artist Similarity', path: '/recommend_artists_by_similarity' },
            { label: 'Artist Stats', path: '/artist_stats' },
            {label: 'Happiest Artists', path: '/happiest_artists' }
          ].map(item => (
            <Grid item key={item.path}>
              <Button
                component={RouterLink}
                to={item.path}
                variant="contained"
              >
                {item.label}
              </Button>
            </Grid>
          ))}
        </Grid>
      </Box>

      {/* Footer */}
      <Box textAlign="center" pt={4}>
        <Typography variant="body2" color="textSecondary">
          &copy; {new Date().getFullYear()} SpotRec. Made with ❤️ using React & Spotify Data.
        </Typography>
      </Box>
    </Container>
  );
}
