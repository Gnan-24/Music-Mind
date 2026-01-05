const LoadingSkeleton = ({ height = '200px' }) => {
  return (
    <div
      style={{
        height,
        borderRadius: '12px',
        background: 'linear-gradient(90deg, #111 25%, #222 37%, #111 63%)',
        backgroundSize: '400% 100%',
        animation: 'skeleton 1.4s ease infinite'
      }}
    />
  );
};

export default LoadingSkeleton;
