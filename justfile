preprocess:
    uv run data_preprocessor.py

simulate:
    uv run simulation.py

visualise:
    uv run visualizer.py

run_pipeline:
    just preprocess && just simulate && just visualise

lint:
    uv run ruff check .
    uv run ruff format --check .
    uv run ty check .

lint-fix:
    uv run ruff check --fix .
    uv run ruff format .

test:
    uv run pytest .

check:
    just lint && just test
