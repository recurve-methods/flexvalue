CREATE TABLE discount (
    pk SERIAL PRIMARY KEY,
    project_id TEXT,
    date DATE,
    timestamp TIMESTAMP,
    discount FLOAT
);
