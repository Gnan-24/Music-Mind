// src/services/api.js
// Phase 2 – Mock API layer (frontend only)

export const getDashboardData = async () => {
  // simulate API delay
  await new Promise((res) => setTimeout(res, 600));

  return {
    genres: [
      { name: 'Pop', percent: 80 },
      { name: 'Rock', percent: 60 },
      { name: 'Hip-Hop', percent: 45 },
      { name: 'Jazz', percent: 30 }
    ],
    friends: [
      { name: 'Alice', match: '95%' },
      { name: 'Bob', match: '82%' },
      { name: 'Charlie', match: '60%' }
    ],
    recommendations: Array(5).fill(null).map((_, i) => ({
      id: i,
      title: `New Gem ${i + 1}`,
      artist: `Undiscovered Artist`
    }))
  };
};
// ===== INSIGHTS =====
export const getInsightsData = async () => {
  await new Promise((res) => setTimeout(res, 500));

  return {
    topSongs: Array(10).fill(null).map((_, i) => ({
      id: i,
      title: `Vibe Song ${i + 1}`,
      artist: `Artist ${i + 1}`,
      duration: '3:20'
    })),
    topGenres: [
      { name: 'Lo-Fi Hip Hop', percent: 85 },
      { name: 'Indie Pop', percent: 70 },
      { name: 'Synthwave', percent: 60 },
      { name: 'Jazz', percent: 40 }
    ],
    topArtists: [
      { id: 1, name: 'The Weeknd' },
      { id: 2, name: 'Daft Punk' },
      { id: 3, name: 'Tame Impala' }
    ]
  };
};

// ===== TOP SONGS =====
export const getTopSongs = async () => {
  await new Promise((res) => setTimeout(res, 500));

  return Array(20).fill(null).map((_, i) => ({
    id: i,
    title: `Song Title ${i + 1}`,
    artist: `Artist Name ${i + 1}`,
    duration: '3:45'
  }));
};
