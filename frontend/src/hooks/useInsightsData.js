import { useEffect, useState } from 'react';
import { getInsightsData } from '../services/api';

const useInsightsData = () => {
  const [data, setData] = useState({
    topSongs: [],
    topGenres: [],
    topArtists: []
  });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchInsights = async () => {
      try {
        const res = await getInsightsData();
        setData(res);
      } catch {
        setError('Failed to load insights');
      } finally {
        setLoading(false);
      }
    };

    fetchInsights();
  }, []);

  return { data, loading, error };
};

export default useInsightsData;
