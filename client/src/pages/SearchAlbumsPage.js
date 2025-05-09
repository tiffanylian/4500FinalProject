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
  Paper
} from '@mui/material';
import { NavLink } from 'react-router-dom';
const config = require('../config.json');

export default function SearchAlbumsPage() {
  const [name, setName] = useState('');
  const [artist, setArtist] = useState('');
  const [limit, setLimit] = useState(20);
  const [results, setResults] = useState([]);

  const handleSearch = async () => {
    const params = new URLSearchParams();
    if (name) params.append('name', name);
    if (artist) params.append('artist', artist);
    params.append('limit', limit);

    try {
      const response = await fetch(
        `http://${config.server_host}:${config.server_port}/search_albums?${params.toString()}`
      );
      const data = await response.json();
      setResults(data);
    } catch (error) {
      console.error('Failed to search albums:', error);
    }
  };

  return (
    <Container sx={{ mt: 4 }}>
      <Typography variant="h4" gutterBottom>
        Search Albums
      </Typography>

      <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: 16 }}>
        <TextField
          label="Album Name"
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
                <TableCell>Album Title</TableCell>
                <TableCell>Artist</TableCell>
                <TableCell align="right">Playlist Count</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {results.map((row) => (
                <TableRow key={row.album_id} hover sx={{ cursor: 'pointer' }}>
                  <TableCell>
                    <NavLink to={`/albums/${row.album_id}`} style={{ textDecoration: 'none', color: 'inherit' }}>
                      {row.album_name}
                    </NavLink>
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