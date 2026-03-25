import { useEffect, useRef, useState } from "react";

type UseApiDataOptions = {
  keepPreviousData?: boolean;
};

export function useApiData<T>(loader: () => Promise<T>, options?: UseApiDataOptions) {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const hasDataRef = useRef(false);

  useEffect(() => {
    hasDataRef.current = data !== null;
  }, [data]);

  useEffect(() => {
    let cancelled = false;

    async function run() {
      const shouldKeepPreviousData = Boolean(options?.keepPreviousData && hasDataRef.current);
      if (shouldKeepPreviousData) {
        setRefreshing(true);
      } else {
        setLoading(true);
      }
      setError(null);
      try {
        const result = await loader();
        if (!cancelled) {
          setData(result);
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Unknown API error");
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
          setRefreshing(false);
        }
      }
    }

    void run();

    return () => {
      cancelled = true;
    };
  }, [loader, options?.keepPreviousData]);

  return { data, loading, refreshing, error };
}
