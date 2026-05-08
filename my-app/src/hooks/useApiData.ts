import { useEffect, useRef, useState } from "react";

type UseApiDataOptions<T> = {
  keepPreviousData?: boolean;
  initialData?: T | null;
  resetKey?: string | number | boolean | null;
};

export function useApiData<T>(loader: () => Promise<T>, options?: UseApiDataOptions<T>) {
  const keepPreviousData = Boolean(options?.keepPreviousData);
  const initialData = options?.initialData ?? null;
  const resetKey = options?.resetKey ?? null;
  const [data, setData] = useState<T | null>(initialData);
  const [loading, setLoading] = useState(initialData ? false : true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const hasDataRef = useRef(initialData !== null);
  const latestInitialDataRef = useRef<T | null>(initialData);

  useEffect(() => {
    hasDataRef.current = data !== null;
  }, [data]);

  useEffect(() => {
    latestInitialDataRef.current = initialData;
  }, [initialData]);

  useEffect(() => {
    const nextInitialData = latestInitialDataRef.current;
    setData(nextInitialData);
    setLoading(nextInitialData === null);
    setRefreshing(false);
    setError(null);
    hasDataRef.current = nextInitialData !== null;
  }, [resetKey]);

  useEffect(() => {
    let cancelled = false;

    async function run() {
      const shouldKeepPreviousData = Boolean(keepPreviousData && hasDataRef.current);
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
          setError(hasDataRef.current ? null : err instanceof Error ? err.message : "Unknown API error");
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
  }, [loader, keepPreviousData]);

  return { data, loading, refreshing, error };
}
