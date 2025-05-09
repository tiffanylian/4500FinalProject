import React from 'react';
import { Link as RouterLink } from 'react-router-dom';
import {
  Container,
  Grid,
  Card,
  CardActionArea,
  CardContent,
  Typography,
  Box
} from '@mui/material';

const recommendations = [
  {
    label: 'Recommend Song on Song',
    path: '/recommend_song_on_song',
    description: 'Get song suggestions based on a selected song.'
  },
  {
    label: 'Recommend Song on Artist',
    path: '/recommend_song_on_artist',
    description: 'Discover songs similar to your favorite artists.'
  },
  {
    label: 'Recommend Song on Playlist',
    path: '/recommend_song_on_playlist',
    description: 'Find tracks that fit your chosen playlist mood.'
  },
  {
    label: 'Recommend Playlist on Song',
    path: '/recommend_playlist_on_song',
    description: 'Explore playlists that match a particular track.'
  },
  {
    label: 'Recommend Artists by Similarity',
    path: '/recommend_artists_by_similarity',
    description: 'Meet new artists who sound like your favorites.'
  },
  {
    label: 'Recommend Happy Artists',
    path: '/happiest_artists',
    description: 'Discover artists with the happiest songs.'
  }

];

export default function RecommendationPage() {
  return (
    <Container maxWidth="lg" sx={{ py: 4 }}>
      <Box textAlign="center" mb={4}>
        <Typography variant="h4" component="h2">
          Recommendations Hub
        </Typography>
        <Typography variant="subtitle1" color="textSecondary">
          Choose a recommendation method to get started
        </Typography>
      </Box>

      <Grid container spacing={4}>
        {recommendations.map(item => (
          <Grid item xs={12} sm={6} md={4} key={item.path}>
            <Card elevation={3}>
              <CardActionArea component={RouterLink} to={item.path}>
                <CardContent>
                  <Typography gutterBottom variant="h6">
                    {item.label}
                  </Typography>
                  <Typography variant="body2" color="textSecondary">
                    {item.description}
                  </Typography>
                </CardContent>
              </CardActionArea>
            </Card>
          </Grid>
        ))}
      </Grid>
    </Container>
  );
}
