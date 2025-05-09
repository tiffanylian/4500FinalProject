import { useState } from 'react';
import {
  Container,
  TextField,
  Button,
  Divider,
  Link,
  TableContainer,
  Table,
  TableHead,
  TableBody,
  TableRow,
  TableCell,
  Paper,
  Typography
} from '@mui/material';
import SongCard from '../components/SongCard';
const config = require('../config.json');

export default function RecommendArtistSongPage() {
  const [artistQuery, setArtistQuery] = useState('');
  const [limit, setLimit] = useState(10);
  const [results, setResults] = useState([]);
  const [selectedSongId, setSelectedSongId] = useState(null);

  const handleSearch = async () => {
    if (!artistQuery) return;
    try {
      const response = await fetch(
        `http://${config.server_host}:${config.server_port}/recommend_song_on_artist?name=${encodeURIComponent(
          artistQuery
        )}&limit=${limit}`
      );
      const data = await response.json();
      setResults(data);
    } catch (error) {
      console.error('Failed to fetch artist-based recommendations:', error);
    }
  };

  return (
    <Container sx={{ mt: 4 }}>
      {selectedSongId && (
        <SongCard songId={selectedSongId} handleClose={() => setSelectedSongId(null)} />
      )}

      <Typography variant="h4" gutterBottom>
        Recommend Songs Based on Artist
      </Typography>

      <div style={{ display: 'flex', gap: 16, alignItems: 'flex-end', marginBottom: 16 }}>
        <TextField
          label="Artist Name"
          value={artistQuery}
          onChange={(e) => setArtistQuery(e.target.value)}
          fullWidth
        />
        <TextField
          label="Limit"
          type="number"
          value={limit}
          onChange={(e) => setLimit(parseInt(e.target.value, 10) || 10)}
          sx={{ width: 100 }}
        />
        <Button variant="contained" onClick={handleSearch}>
          Search
        </Button>
      </div>

      <Divider />

      {results.length > 0 && (
        <TableContainer component={Paper} sx={{ mt: 3, boxShadow: 3 }}>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>Song Title</TableCell>
                <TableCell>Artists</TableCell>
                <TableCell align="right">Similarity</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {results.map((row) => (
                <TableRow key={row.track_id} hover sx={{ cursor: 'pointer' }}>
                  <TableCell>
                    <Link onClick={() => setSelectedSongId(row.track_id)}>
                      {row.name}
                    </Link>
                  </TableCell>
                  <TableCell>{row.artist_names}</TableCell>
                  <TableCell align="right">{row.similarity}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      )}
    </Container>
  );
}