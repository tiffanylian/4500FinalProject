import { useState } from 'react';
import {
  Container,
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
  Typography
} from '@mui/material';
import { NavLink } from 'react-router-dom';
const config = require('../config.json');

export default function RecommendArtistsBySimilarityPage() {
  const [artistQuery, setArtistQuery] = useState('');
  const [limit, setLimit] = useState(10);
  const [results, setResults] = useState([]);

  const handleSearch = async () => {
    if (!artistQuery) return;
    try {
      const response = await fetch(
        `http://${config.server_host}:${config.server_port}/recommend_artists_by_similarity?name=${encodeURIComponent(
          artistQuery
        )}&limit=${limit}`
      );
      const data = await response.json();
      setResults(data);
    } catch (error) {
      console.error('Failed to fetch artist similarity recommendations:', error);
    }
  };

  return (
    <Container sx={{ mt: 4 }}>
      <Typography variant="h4" gutterBottom>
        Recommend Similar Artists
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
                <TableCell>Artist Name</TableCell>
                <TableCell align="right">Similarity</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {results.map((row) => (
                <TableRow key={row.artist_id} hover>
                  <TableCell>
                    <NavLink to={`/artists/${row.artist_id}`} style={{ textDecoration: 'none', color: 'inherit' }}>
                      {row.name}
                    </NavLink>
                  </TableCell>
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
