-- Allow pgmq.meta to be dumped by `pg_dump` when pgmq is installed as an extension
DO
$$
BEGIN
    IF EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'pgmq') THEN
        PERFORM pg_catalog.pg_extension_config_dump('pgmq.meta', '');
    END IF;
END
$$;