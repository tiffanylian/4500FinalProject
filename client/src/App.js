import { BrowserRouter, Routes, Route } from "react-router-dom";
import { CssBaseline, ThemeProvider } from '@mui/material'
import {blue, green, red } from '@mui/material/colors'
import { createTheme } from "@mui/material/styles";

import NavBar from './components/NavBar';
import TopSongsPage from './pages/TopSongsPage';
import TopAlbumsPage from './pages/TopAlbumsPage.js';
import TopPlaylistsPage from './pages/TopPlaylistsPage.js';
import HomePage from './pages/HomePage';
import RecommendationPage from './pages/recommendationPage.js';
import RecommendSongonSongPage from './pages/RecommendSongonSong.js';
import RecommendSongonArtistPage from './pages/RecommendSongonArtist.js';
import RecommendSongonPlaylistPage from './pages/RecommendSongonPlaylist.js';
import RecommendPlaylistonSongPage from './pages/RecommendPlaylistonSong.js';
import RecommendArtistsBySimilarityPage from './pages/RecommendArtistsbySimilarity.js';
import SearchSongsPage from './pages/SearchSongsPage.js';
import SearchAlbumsPage from './pages/SearchAlbumsPage.js';
import SearchPlaylistsPage from "./pages/SearchPlaylistsPage.js";
import ArtistStatsPage from "./pages/ArtistStatsPage.js";
import HappiestArtistsPage from "./pages/ArtistwithHappiestSongsPage.js";


// createTheme enables you to customize the look and feel of your app past the default
// in this case, we only change the color scheme
export const theme = createTheme({
  palette: {
    primary: green,
    secondary: red,
  },
});

// App is the root component of our application and as children contain all our pages
// We use React Router's BrowserRouter and Routes components to define the pages for
// our application, with each Route component representing a page and the common
// NavBar component allowing us to navigate between pages (with hyperlinks)
export default function App() {
  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <BrowserRouter>
        <NavBar />
        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/top_songs" element={<TopSongsPage />} />
          <Route path="/top_albums" element={<TopAlbumsPage />} />
          <Route path="/top_playlists" element={<TopPlaylistsPage />} />
          <Route path="/recommendation" element={<RecommendationPage />} />
          <Route path="/recommend_song_on_song" element={<RecommendSongonSongPage />} />
          <Route path="/recommend_song_on_artist" element={<RecommendSongonArtistPage />} />
          <Route path="/recommend_song_on_playlist" element={<RecommendSongonPlaylistPage />} />
          <Route path="/recommend_playlist_on_song" element={<RecommendPlaylistonSongPage />} />
          <Route path="/recommend_artists_by_similarity" element={<RecommendArtistsBySimilarityPage />} />
          <Route path="/search_songs" element={<SearchSongsPage />} />
          <Route path="/search_albums" element={<SearchAlbumsPage />} />
          <Route path="/search_playlists" element={<SearchPlaylistsPage />} />
          <Route path="/artist_stats" element={<ArtistStatsPage />} />
          <Route path="/happiest_artists" element={<HappiestArtistsPage />} />
        </Routes>
      </BrowserRouter>
    </ThemeProvider>
  );
}