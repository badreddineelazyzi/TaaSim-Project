CREATE TABLE IF NOT EXISTS taasim.kpi_aggregates (
    kpi_name text,
    category text,
    subcategory text,
    label text,
    value double,
    PRIMARY KEY ((kpi_name), category, subcategory)
);
