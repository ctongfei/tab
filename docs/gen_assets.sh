mkdir -p docs/assets
uv run tab view docs/test.csv -o table-svg 2> docs/assets/test.svg
uv run tab sql 'SELECT * FROM t WHERE Metric_A_Value > 80' docs/test.csv -o table-svg 2> docs/assets/test-where.svg
