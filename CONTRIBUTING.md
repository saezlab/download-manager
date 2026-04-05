# Contributing to dlmachine

Thanks for contributing to `dlmachine`.

## Ground Rules

- Use English in issues and pull requests.
- Be respectful and constructive in discussions.
- Before opening a new issue, search existing issues first.

## Development Setup

1. Fork the repository and clone your fork.
2. Install dependencies:
   ```bash
   poetry install
   ```
3. (Optional but recommended) Install pre-commit hooks:
   ```bash
   pre-commit install
   ```

## Running Checks Locally

- Run tests:
  ```bash
  poetry run pytest
  ```
- Run lint checks:
  ```bash
  poetry run flake8 dlmachine
  ```

## Branches and Commits

- Create a feature branch from `main`.
- Keep commits focused and small.
- Use clear commit messages that explain intent.

## Pull Request Process

1. Ensure tests and lint pass locally.
2. Update docs/tests when behavior changes.
3. Fill out the pull request template completely.
4. Link related issues (for example: `Closes #123`).

## Reporting Bugs and Requesting Features

Use the issue templates in `.github/ISSUE_TEMPLATE`:

- Bug report
- Documentation improvement
- Feature request

## Questions

If you're unsure about scope or implementation details, open an issue first so we can align before coding.
