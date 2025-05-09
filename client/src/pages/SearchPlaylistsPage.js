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

export default function SearchPlaylistsPage() {
  const [name, setName] = useState('');
  const [limit, setLimit] = useState(20);
  const [results, setResults] = useState([]);

  const handleSearch = async () => {
    const params = new URLSearchParams();
    params.append('name', name);
    params.append('limit', limit);

    try {
      const response = await fetch(
        `http://${config.server_host}:${config.server_port}/search_playlists?${params.toString()}`
      );
      const data = await response.json();
      setResults(data);
    } catch (error) {
      console.error('Failed to search playlists:', error);
    }
  };

  return (
    <Container sx={{ mt: 4 }}>
      <Typography variant="h4" gutterBottom>
        Search Playlists
      </Typography>

      <div style={{ display: 'flex', gap: 16, alignItems: 'flex-end', marginBottom: 16 }}>
        <TextField
          label="Playlist Name"
          value={name}
          onChange={(e) => setName(e.target.value)}
          fullWidth
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
                <TableCell>Playlist Name</TableCell>
                <TableCell align="right">Song Count</TableCell>
                <TableCell align="right">Followers</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {results.map((row) => (
                <TableRow key={row.playlist_id} hover>
                  <TableCell>
                    <NavLink
                      to={`/playlists/${row.playlist_id}`}
                      style={{ textDecoration: 'none', color: 'inherit' }}
                    >
                      {row.name}
                    </NavLink>
                  </TableCell>
                  <TableCell align="right">{row.song_count}</TableCell>
                  <TableCell align="right">{row.followers}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      )}
    </Container>
  );
}