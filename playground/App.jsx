import { useState } from 'react';
import init, { exec_query } from './wasm';

function App() {
  const [rows, setRows] = useState('');
  const [query, setQuery] = useState('');
  const execQuery = (q) => {
    init().then(() => {
        setRows(exec_query(q));
    })
  }
  return (
    <div className="App">
      <h1> Snowflake Playground </h1>
      <h2> Query </h2>
      <input type="text" name="query" onChange={(e) => setQuery(e.target.value)} />
      <button onClick={() => execQuery(query)}> Execute Query </button>
      <h1> `Result {rows}` </h1>
    </div>
  );
}

export default App;
