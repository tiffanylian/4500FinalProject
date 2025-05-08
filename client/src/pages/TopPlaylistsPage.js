import { useEffect, useState } from 'react';
import { Container, Divider, Link } from '@mui/material';

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

  // Here, we define the columns of the "Top Songs" table. The songColumns variable is an array (in order)
  // of objects with each object representing a column. Each object has a "field" property representing
  // what data field to display from the raw data, "headerName" property representing the column label,
  // and an optional renderCell property which given a row returns a custom JSX element to display in the cell.
  const playlistColumns = [
    {
      field: 'name',
      headerName: 'Playlist Title',
      renderCell: (row) => <Link onClick={() => setSelectedSongId(row.playlist_id)}>{row.name}</Link> // A Link component is used just for formatting purposes
    },
    {
      field: 'followers',
      headerName: 'Playlist Followers',
    },
  ];


  return (
    <Container>
      {/* SongCard is a custom component that we made. selectedSongId && <SongCard .../> makes use of short-circuit logic to only render the SongCard if a non-null song is selected */}
      {selectedSongId && <SongCard songId={selectedSongId} handleClose={() => setSelectedSongId(null)} />}
      <h2>Here is all the top songs of all time!:&nbsp;
        <Link onClick={() => setSelectedSongId(songOfTheDay.song_id)}>{songOfTheDay.title}</Link>
      </h2>
      <Divider />
      <h2>Top Songs</h2>
      <LazyTable route={`http://${config.server_host}:${config.server_port}/top_playlists`} columns={playlistColumns} defaultPageSize={5} rowsPerPageOptions={[5, 10]}/>
      <Divider />
      {/* TODO (TASK 17): add a paragraph (<p></p>) that displays “Created by [name]” using the name state stored from TASK 13/TASK 14 */}
      <p>Created by {appAuthor}</p>
    </Container>
  );
};