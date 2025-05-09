import { useState, useEffect } from 'react';
import {
  Container,
  Typography,
  TableContainer,
  Table,
  TableHead,
  TableBody,
  TableRow,
  TableCell,
  Paper,
  Divider
} from '@mui/material';
import { NavLink } from 'react-router-dom';
const config = require('../config.json');

export default function HappiestArtistsPage() {
  const [artists, setArtists] = useState([]);

  useEffect(() => {
    const fetchHappiest = async () => {
      try {
        const res = await fetch(
          `http://${config.server_host}:${config.server_port}/happiest_artists`
        );
        const data = await res.json();
        setArtists(data);
      } catch (err) {
        console.error('Failed to fetch happiest artists:', err);
      }
    };
    fetchHappiest();
  }, []);

  return (
    <Container sx={{ mt: 4 }}>
      <Typography variant="h4" gutterBottom>
        Artists with Happiest Songs
      </Typography>
      <Divider sx={{ mb: 2 }} />
      <TableContainer component={Paper} sx={{ boxShadow: 3 }}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Artist Name</TableCell>
              <TableCell align="right">Total Tracks</TableCell>
              <TableCell>Top Album</TableCell>
              <TableCell align="right">Top Album Valence</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {artists.map((row) => (
              <TableRow key={row.artist_id} hover>
                <TableCell>
                  <NavLink
                    to={`/artists/${row.artist_id}`}
                    style={{ textDecoration: 'none', color: 'inherit' }}
                  >
                    {row.artist_name}
                  </NavLink>
                </TableCell>
                <TableCell align="right">{row.total_tracks}</TableCell>
                <TableCell>
                  <NavLink
                    to={`/albums/${row.album_id}`}
                    style={{ textDecoration: 'none', color: 'inherit' }}
                  >
                    {row.album_name}
                  </NavLink>
                </TableCell>
                <TableCell align="right">
                  {parseFloat(row.top_album_valence).toFixed(3)}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Container>
  );
}