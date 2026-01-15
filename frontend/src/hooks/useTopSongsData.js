import { useEffect, useState } from 'react';
import { getTopSongs } from '../services/api';

const useTopSongsData = () => {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchSongs = async () => {
      try {
        const res = await getTopSongs();
        setData(res);
      } catch {
        setError('Failed to load top songs');
      } finally {
        setLoading(false);
      }
    };

    fetchSongs();
  }, []);

  return { data, loading, error };
};

export default useTopSongsData;
