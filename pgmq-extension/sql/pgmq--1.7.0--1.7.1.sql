CREATE OR REPLACE FUNCTION pgmq._extension_exists(extension_name TEXT)
RETURNS BOOLEAN
LANGUAGE SQL AS $$
SELECT EXISTS (
    SELECT 1
    FROM pg_extension
    WHERE extname = extension_name
)
$$;
