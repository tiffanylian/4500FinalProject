import { useState } from 'react';
import {
  Container,
  Typography,
  TextField,
  Button,
  Divider,
  TableContainer,
  Table,
  TableHead,
  TableBody,
  TableRow,
  TableCell,
  Paper,
  Link
} from '@mui/material';
import SongCard from '../components/SongCard';
const config = require('../config.json');

export default function ArtistStatsPage() {
  const [artistQuery, setArtistQuery] = useState('');
  const [stats, setStats] = useState(null);
  const [selectedSongId, setSelectedSongId] = useState(null);

  const handleSearch = async () => {
    if (!artistQuery) return;
    try {
      const response = await fetch(
        `http://${config.server_host}:${config.server_port}/artist_stats?name=${encodeURIComponent(
          artistQuery
        )}`
      );
      const data = await response.json();
      setStats(data);
    } catch (error) {
      console.error('Failed to fetch artist stats:', error);
    }
  };

  return (
    <Container sx={{ mt: 4 }}>
      <Typography variant="h4" gutterBottom>
        Artist Statistics
      </Typography>

      <div style={{ display: 'flex', gap: 16, alignItems: 'flex-end', marginBottom: 16 }}>
        <TextField
          label="Artist Name"
          value={artistQuery}
          onChange={(e) => setArtistQuery(e.target.value)}
          fullWidth
        />
        <Button variant="contained" onClick={handleSearch}>
          Search
        </Button>
      </div>

      <Divider />

      {stats && (
        <>
          <Typography variant="h5" sx={{ mt: 2 }}>
            {stats.artist_name}
          </Typography>

          {/* Top Song */}
          <Typography variant="h6" sx={{ mt: 2 }}>
            Top Song
          </Typography>
          <Link
            component="button"
            variant="body1"
            onClick={() => setSelectedSongId(stats.top_song.track_id)}
          >
            {stats.top_song.name} ({stats.top_song.playlist_count} plays)
          </Link>

          {selectedSongId && (
            <SongCard songId={selectedSongId} handleClose={() => setSelectedSongId(null)} />
          )}

          {/* Stats by Year */}
          <Typography variant="h6" sx={{ mt: 4 }}>Stats by Year</Typography>
          <TableContainer component={Paper} sx={{ mt: 1, boxShadow: 3 }}>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>Year</TableCell>
                  <TableCell align="right">Avg Danceability</TableCell>
                  <TableCell align="right">Avg Energy</TableCell>
                  <TableCell align="right">Avg Valence</TableCell>
                  <TableCell align="right">Avg Acousticness</TableCell>
                  <TableCell align="right">Avg Loudness</TableCell>
                  <TableCell align="right">Playlist Count</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {stats.stats_by_year.map((row) => (
                  <TableRow key={row.year}>
                    <TableCell>{row.year}</TableCell>
                    <TableCell align="right">{row.avg_danceability.toFixed(3)}</TableCell>
                    <TableCell align="right">{row.avg_energy.toFixed(3)}</TableCell>
                    <TableCell align="right">{row.avg_valence.toFixed(3)}</TableCell>
                    <TableCell align="right">{row.avg_acousticness.toFixed(3)}</TableCell>
                    <TableCell align="right">{row.avg_loudness.toFixed(3)}</TableCell>
                    <TableCell align="right">{row.playlist_count}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </>
      )}
    </Container>
  );
}