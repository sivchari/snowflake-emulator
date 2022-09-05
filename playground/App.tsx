import { ChangeEvent, useState } from 'react';
import init, { exec_query } from './wasm';

const App: React.FC = () => {
  const [rows, setRows] = useState('');
  const [query, setQuery] = useState('');
  const execQuery = (q: string) => {
    init().then(() => {
      setRows(exec_query(q));
    })
  }
  return (
    <div className="App">
      <h1> Snowflake Playground </h1>
      <h2> Query </h2>
      <input type="text" name="query" onChange={(e: ChangeEvent<HTMLInputElement>) => setQuery(e.target.value)} />
      <button onClick={() => execQuery(query)}> Execute Query </button>
      <h1> Result </h1>
      <h2> {rows} </h2>
    </div>
  );
}

export default App;
