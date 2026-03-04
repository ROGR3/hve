preprocess:
    uv run data_preprocessor.py

simulate:
    uv run simulation.py

visualise:
    uv run visualizer.py

run_pipeline:
    just preprocess && just simulate && just visualise

lint:
    uv run isort .
    uv run mypy .
    uv run pylint $(find . -name "*.py" -not -path "./.venv/*")
    uv run black .

test:
    uv run pytest .

check:
    just lint && just test
