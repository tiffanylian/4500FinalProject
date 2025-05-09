import { useState } from 'react';
import {
  Container,
  Typography,
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
  Paper
} from '@mui/material';
import SongCard from '../components/SongCard';
const config = require('../config.json');

export default function SearchSongsPage() {
  const [name, setName] = useState('');
  const [artist, setArtist] = useState('');
  const [year, setYear] = useState('');
  const [limit, setLimit] = useState(20);
  const [results, setResults] = useState([]);
  const [selectedSongId, setSelectedSongId] = useState(null);

  const handleSearch = async () => {
    const params = new URLSearchParams();
    if (name) params.append('name', name);
    if (artist) params.append('artist', artist);
    if (year) params.append('year', year);
    params.append('limit', limit);

    try {
      const response = await fetch(
        `http://${config.server_host}:${config.server_port}/search_songs?${params.toString()}`
      );
      const data = await response.json();
      setResults(data);
    } catch (error) {
      console.error('Failed to search songs:', error);
    }
  };

  return (
    <Container sx={{ mt: 4 }}>
      {selectedSongId && (
        <SongCard songId={selectedSongId} handleClose={() => setSelectedSongId(null)} />
      )}

      <Typography variant="h4" gutterBottom>
        Search Songs
      </Typography>

      <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: 16 }}>
        <TextField
          label="Song Name"
          value={name}
          onChange={(e) => setName(e.target.value)}
          sx={{ flex: 1, minWidth: 200 }}
        />
        <TextField
          label="Artist"
          value={artist}
          onChange={(e) => setArtist(e.target.value)}
          sx={{ flex: 1, minWidth: 200 }}
        />
        <TextField
          label="Year"
          value={year}
          onChange={(e) => setYear(e.target.value)}
          sx={{ width: 100 }}
        />
        <TextField
          label="Limit"
          type="number"
          value={limit}
          onChange={(e) => setLimit(parseInt(e.target.value, 10) || 20)}
          sx={{ width: 100 }}
        />
        <Button variant="contained" onClick={handleSearch} sx={{ height: 40 }}>
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
                <TableCell>Artist</TableCell>
                <TableCell align="right">Playlist Count</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {results.map((row) => (
                <TableRow key={row.track_id} hover sx={{ cursor: 'pointer' }}>
                  <TableCell>
                    <Link onClick={() => setSelectedSongId(row.track_id)}>
                      {row.track_name}
                    </Link>
                  </TableCell>
                  <TableCell>{row.artist_name}</TableCell>
                  <TableCell align="right">{row.playlist_count}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      )}
    </Container>
  );
}