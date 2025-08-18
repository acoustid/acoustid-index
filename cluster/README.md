# fpindex-cluster

## Development

### Code Quality

This project uses [ruff](https://ruff.rs/) for linting and source code formatting, and [ty](https://github.com/astral-sh/ty) for type checking.

To check for linting issues:
```bash
uv run ruff check .
```

To automatically fix linting issues:
```bash
uv run ruff check --fix .
```

To format code:
```bash
uv run ruff format .
```

To run type checking:
```bash
uv run ty check
```

To run tests:
```bash
uv run pytest
```

To run all code quality checks at once:
```bash
./check.sh
```