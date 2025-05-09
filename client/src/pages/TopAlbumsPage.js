import { useEffect, useState } from 'react';
import { Container, Divider, Link } from '@mui/material';
import { NavLink } from 'react-router-dom';

import LazyTable from '../components/LazyTable';
import SongCard from '../components/SongCard';
const config = require('../config.json');

export default function HomePage() {
  // We use the setState hook to persist information across renders (such as the result of our API calls)
  const [songOfTheDay, setSongOfTheDay] = useState({});
  // TODO (TASK 13): add a state variable to store the app author (default to '')
  const [appAuthor, setAppAuthor] = useState('');
  const [selectedSongId, setSelectedSongId] = useState(null);

  // The useEffect hook by default runs the provided callback after every render
  // The second (optional) argument, [], is the dependency array which signals
  // to the hook to only run the provided callback if the value of the dependency array
  // changes from the previous render. In this case, an empty array means the callback
  // will only run on the very first render.
  useEffect(() => {
    // Fetch request to get the song of the day. Fetch runs asynchronously.
    // The .then() method is called when the fetch request is complete
    // and proceeds to convert the result to a JSON which is finally placed in state.
    fetch(`http://${config.server_host}:${config.server_port}/random`)
      .then(res => res.json())
      .then(resJson => setSongOfTheDay(resJson));

      fetch(`http://${config.server_host}:${config.server_port}/home`)
  .then(res => res.json())
  .then(({ authors }) => {
    const prettyAuthors = authors.join(', ');
    setAppAuthor(prettyAuthors);
  })
  .catch(err => console.error('Failed to fetch authors:', err));
  }, []);


  // TODO (TASK 15): define the columns for the top albums (schema is Album Title, Plays), where Album Title is a link to the album page
  // Hint: this should be very similar to songColumns defined above, but has 2 columns instead of 3
  // Hint: recall the schema for an album is different from that of a song (see the API docs for /top_albums). How does that impact the "field" parameter and the "renderCell" function for the album title column?
  const albumColumns = [
    {
      field: 'album_name',
      headerName: 'Album Title',
    renderCell: (row) => <Link onClick={() => setSelectedSongId(row.album_id)}>{row.album_name}</Link> // A Link component is used just for formatting purposes
    },
    {
      field: 'artist_name',
      headerName: 'Artist'
    },
    {
      field: 'playlist_count',
      headerName: 'Playlists'
    }
  ];

  return (
    <Container>
      {/* SongCard is a custom component that we made. selectedSongId && <SongCard .../> makes use of short-circuit logic to only render the SongCard if a non-null song is selected */}
      {selectedSongId && <SongCard songId={selectedSongId} handleClose={() => setSelectedSongId(null)} />}
      <h2>Check out your song of the day:&nbsp;
        <Link onClick={() => setSelectedSongId(songOfTheDay.song_id)}>{songOfTheDay.title}</Link>
      </h2>
      <Divider />
      {/* TODO (TASK 16): add a h2 heading, LazyTable, and divider for top albums. Set the LazyTable's props for defaultPageSize to 5 and rowsPerPageOptions to [5, 10] */}
      <h2>Top Albums</h2>
      <LazyTable route={`http://${config.server_host}:${config.server_port}/top_albums`} columns={albumColumns} defaultPageSize={5} rowsPerPageOptions={[5, 10]}/>
      <Divider />
      {/* TODO (TASK 17): add a paragraph (<p></p>) that displays “Created by [name]” using the name state stored from TASK 13/TASK 14 */}
      <p>Created by {appAuthor}</p>
    </Container>
  );
};